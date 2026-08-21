//! Cross-section validation for parsed server configuration.
//!
//! Checks append errors in declaration order so one startup attempt reports every independent
//! problem without changing the stable diagnostic ordering.

use super::*;

/// Validate the startup-bound HTTP authentication boundary.
///
/// This is separate from full file validation so programmatic startup paths can enforce the
/// same credential and bind invariants before creating any externally visible resources.
pub(crate) fn validate_http_auth(config: &ServerConfig) -> Result<(), ConfigError> {
    let mut errors = Vec::new();
    collect_http_auth_errors(config, &mut errors);
    if errors.is_empty() {
        Ok(())
    } else {
        Err(ConfigError::ValidationErrors { errors })
    }
}

fn collect_http_auth_errors(config: &ServerConfig, errors: &mut Vec<String>) {
    let bind = match config.server.bind.parse::<std::net::SocketAddr>() {
        Ok(bind) => Some(bind),
        Err(_) => {
            errors.push(format!(
                "invalid server bind address: '{}'",
                config.server.bind
            ));
            None
        }
    };

    let Some(diagnostic_token) = &config.server.diagnostic_read_token else {
        if let Some(console_token) = &config.server.console_token {
            if console_token.len() < MIN_CONSOLE_TOKEN_LEN {
                errors.push(format!(
                    "server.console_token must be at least {MIN_CONSOLE_TOKEN_LEN} characters"
                ));
            }
        }
        return;
    };

    if config.server.mode != ServerMode::Cluster {
        errors.push("server.diagnostic_read_token requires server.mode = \"cluster\"".to_string());
    }

    match bind {
        Some(bind) if !bind.ip().is_loopback() => errors.push(
            "server.diagnostic_read_token requires server.bind to be a loopback socket address"
                .to_string(),
        ),
        Some(_) | None => {}
    }

    if !is_canonical_http_auth_token(diagnostic_token) {
        errors.push(format!(
            "server.diagnostic_read_token must be the canonical unpadded base64url encoding of \
             exactly {HTTP_AUTH_TOKEN_BYTES} bytes ({HTTP_AUTH_TOKEN_ENCODED_LEN} characters)"
        ));
    }

    match &config.server.console_token {
        None => errors.push(
            "server.diagnostic_read_token requires server.console_token to be configured"
                .to_string(),
        ),
        Some(console_token) => {
            if !is_canonical_http_auth_token(console_token) {
                errors.push(format!(
                    "server.console_token must be the canonical unpadded base64url encoding of \
                     exactly {HTTP_AUTH_TOKEN_BYTES} bytes ({HTTP_AUTH_TOKEN_ENCODED_LEN} \
                     characters) when server.diagnostic_read_token is configured"
                ));
            }
            if console_token == diagnostic_token {
                errors.push(
                    "server.diagnostic_read_token must differ from server.console_token"
                        .to_string(),
                );
            }
        }
    }
}

fn is_canonical_http_auth_token(token: &Secret) -> bool {
    let encoded = token.expose();
    if encoded.len() != HTTP_AUTH_TOKEN_ENCODED_LEN {
        return false;
    }

    match URL_SAFE_NO_PAD.decode(encoded) {
        Ok(decoded) if decoded.len() == HTTP_AUTH_TOKEN_BYTES => {
            URL_SAFE_NO_PAD.encode(decoded) == encoded
        }
        Ok(_) | Err(_) => false,
    }
}

pub(super) fn validate_config(config: &ServerConfig) -> Result<(), ConfigError> {
    let mut errors = Vec::new();

    collect_connector_graph_errors(config, &mut errors);
    collect_http_auth_errors(config, &mut errors);
    collect_pgwire_errors(config, &mut errors);
    collect_cors_errors(config, &mut errors);
    collect_delivery_errors(config, &mut errors);
    collect_runtime_limit_errors(config, &mut errors);
    validate_ai(config, &mut errors);
    validate_cluster_tls(config, &mut errors);

    if errors.is_empty() {
        Ok(())
    } else {
        Err(ConfigError::ValidationErrors { errors })
    }
}

fn collect_connector_graph_errors(config: &ServerConfig, errors: &mut Vec<String>) {
    let pipeline_names: HashSet<&str> = config.pipelines.iter().map(|p| p.name.as_str()).collect();
    for sink in &config.sinks {
        if !pipeline_names.contains(sink.pipeline.as_str()) {
            errors.push(format!(
                "sink '{}' references unknown pipeline '{}'",
                sink.name, sink.pipeline
            ));
        }
        if sink
            .properties
            .keys()
            .any(|key| key.eq_ignore_ascii_case("format"))
        {
            errors.push(format!(
                "sink '{}': format must be configured as a top-level sink field, not under sink.properties",
                sink.name
            ));
        }
        collect_connector_property_errors("sink", &sink.name, &sink.properties, errors);
    }

    let mut seen_sources = HashSet::new();
    for source in &config.sources {
        if !seen_sources.insert(&source.name) {
            errors.push(format!("duplicate source name: '{}'", source.name));
        }
        if source
            .properties
            .keys()
            .any(|key| key.eq_ignore_ascii_case("format"))
        {
            errors.push(format!(
                "source '{}': format must be configured as a top-level source field, not under source.properties",
                source.name
            ));
        }
        collect_connector_property_errors("source", &source.name, &source.properties, errors);
    }

    collect_duplicate_names(
        "pipeline",
        config
            .pipelines
            .iter()
            .map(|pipeline| pipeline.name.as_str()),
        errors,
    );
    collect_duplicate_names(
        "sink",
        config.sinks.iter().map(|sink| sink.name.as_str()),
        errors,
    );
    collect_duplicate_names(
        "lookup",
        config.lookups.iter().map(|lookup| lookup.name.as_str()),
        errors,
    );
}

fn collect_duplicate_names<'a>(
    kind: &str,
    names: impl IntoIterator<Item = &'a str>,
    errors: &mut Vec<String>,
) {
    let mut seen = HashSet::new();
    for name in names {
        if !seen.insert(name) {
            errors.push(format!("duplicate {kind} name: '{name}'"));
        }
    }
}

fn collect_pgwire_errors(config: &ServerConfig, errors: &mut Vec<String>) {
    let server = &config.server;
    if let Some(addr) = &server.pgwire_bind {
        match addr.parse::<std::net::SocketAddr>() {
            Ok(addr) if !addr.ip().is_loopback() && server.pgwire_tls_cert.is_none() => {
                errors.push(
                    "non-loopback pgwire_bind requires pgwire_tls_cert + pgwire_tls_key"
                        .to_string(),
                );
            }
            Ok(_) => {}
            Err(_) => errors.push(format!("invalid server pgwire_bind address: '{addr}'")),
        }
    }

    for (user, password) in &server.pgwire_users {
        if user.is_empty() {
            errors.push("pgwire_users contains an empty username".to_string());
        }
        let password_value = password.expose();
        if let Some(hash) = password_value.strip_prefix("md5") {
            // WHY: strict pg_authid shape prevents a typo from being accepted as plaintext.
            let valid =
                hash.len() == 32 && hash.chars().all(|c| matches!(c, '0'..='9' | 'a'..='f'));
            if !valid {
                errors.push(format!(
                    "pgwire_users['{user}']: pre-hashed value must be 'md5' \
                     followed by 32 lowercase hex characters"
                ));
            }
        } else if password.len() < MIN_PGWIRE_PASSWORD_LEN {
            errors.push(format!(
                "pgwire_users['{user}']: password must be at least {MIN_PGWIRE_PASSWORD_LEN} characters"
            ));
        }
    }

    if server.pgwire_max_connections == 0 {
        errors.push(
            "pgwire_max_connections must be > 0; remove pgwire_bind to disable the listener"
                .to_string(),
        );
    }
    match (&server.pgwire_tls_cert, &server.pgwire_tls_key) {
        (Some(_), None) | (None, Some(_)) => {
            errors.push("pgwire_tls_cert and pgwire_tls_key must be set together".to_string());
        }
        (Some(cert), Some(key)) => {
            if !cert.exists() {
                errors.push(format!("pgwire_tls_cert not found: {}", cert.display()));
            }
            if !key.exists() {
                errors.push(format!("pgwire_tls_key not found: {}", key.display()));
            }
        }
        (None, None) => {}
    }
    match server.pgwire_tls_min_version.as_str() {
        "1.2" | "1.3" => {}
        other => errors.push(format!(
            "pgwire_tls_min_version must be \"1.2\" or \"1.3\" (got \"{other}\")"
        )),
    }
    if let Some(ca) = &server.pgwire_tls_client_ca {
        if server.pgwire_tls_cert.is_none() {
            errors.push(
                "pgwire_tls_client_ca requires pgwire_tls_cert + pgwire_tls_key (mTLS \
                 layers on top of server TLS)"
                    .to_string(),
            );
        }
        if !ca.exists() {
            errors.push(format!("pgwire_tls_client_ca not found: {}", ca.display()));
        }
    }
}

fn collect_cors_errors(config: &ServerConfig, errors: &mut Vec<String>) {
    let Some(origins) = &config.server.console_cors_allowed_origins else {
        return;
    };
    for origin in origins {
        // COMPAT: each configured origin is emitted as an HTTP header value.
        if origin.parse::<axum::http::HeaderValue>().is_err() {
            errors.push(format!(
                "invalid origin in server.console_cors_allowed_origins: '{origin}'"
            ));
        }
    }
}

fn collect_delivery_errors(config: &ServerConfig, errors: &mut Vec<String>) {
    let checkpoint_scope = CheckpointStorageScope::for_url(&config.checkpoint.url);
    if config.server.mode == ServerMode::Cluster {
        if config.server.delivery == DeliveryGuarantee::BestEffort {
            errors.push(
                "cluster mode requires at_least_once delivery; best_effort has no defined \
                 rebalance/state-loss contract"
                    .to_string(),
            );
        }
        if config.discovery.is_none() {
            errors.push("mode = \"cluster\" requires a [discovery] section".to_string());
        }
        if config.node_id.is_none() {
            errors.push("mode = \"cluster\" requires node_id to be set".to_string());
        }
        // WHY: below 100ms the capture-quorum round trip dominates the barrier.
        if config.checkpoint.interval < Duration::from_millis(100) {
            errors.push(format!(
                "mode = \"cluster\": checkpoint.interval = {:?} is too tight; minimum is 100ms",
                config.checkpoint.interval,
            ));
        }
        if checkpoint_scope != CheckpointStorageScope::ClusterShared {
            errors.push(format!(
                "mode = \"cluster\" requires ClusterShared [checkpoint] storage for manifests \
                 and decisions; configured scope is {checkpoint_scope:?}. Use s3://, gs://, or \
                 az:// storage"
            ));
        }
    } else if config.server.delivery == DeliveryGuarantee::ExactlyOnce {
        if !config.checkpoint.url.starts_with("file://") {
            errors.push(
                "[LDB-0014] embedded/single-node exactly-once currently requires a local \
                 file:// checkpoint namespace protected by an exclusive process lock; shared \
                 object-store checkpoints require a term-fenced deployment lease"
                    .to_string(),
            );
        }
        if checkpoint_scope == CheckpointStorageScope::Volatile {
            errors.push(format!(
                "exactly-once delivery requires at least NodeDurable [checkpoint] storage; \
                 configured scope is {checkpoint_scope:?}"
            ));
        }
    } else if config.server.delivery == DeliveryGuarantee::AtLeastOnce
        && checkpoint_scope == CheckpointStorageScope::Volatile
    {
        errors.push(format!(
            "at-least-once delivery requires at least NodeDurable [checkpoint] storage \
             before source acknowledgements can advance; configured scope is \
             {checkpoint_scope:?}"
        ));
    }
}

fn collect_runtime_limit_errors(config: &ServerConfig, errors: &mut Vec<String>) {
    // WHY: zero pauses barrier admission permanently and wedges checkpointing.
    if config.checkpoint.interval.is_zero() {
        errors.push("checkpoint.interval must be > 0".to_string());
    }
    if config.checkpoint.timeout.is_zero() {
        errors.push("checkpoint.timeout must be > 0".to_string());
    }
    if config.checkpoint.max_node_data_bytes == Some(0) {
        errors.push("checkpoint.max_node_data_bytes must be > 0".to_string());
    }
    if let Err(error) = config
        .server
        .validated_temporal_join_idle_history_retention()
    {
        errors.push(format!("server.{error}"));
    }
    if let Err(error) = config.server.validated_source_idle_timeout() {
        errors.push(format!("server.{error}"));
    }
    if let Err(error) = config.server.validated_event_time_max_future_skew() {
        errors.push(format!("server.{error}"));
    }
    // WHY: zero prunes all prior timestamps, so the restart-rate budget never trips.
    if config.supervision.window_secs == Some(0) {
        errors.push("supervision.window_secs must be > 0".to_string());
    }
}

fn collect_connector_property_errors(
    kind: &str,
    name: &str,
    properties: &toml::Table,
    errors: &mut Vec<String>,
) {
    for (key, value) in properties {
        if !connector_property_is_flat(value) {
            errors.push(format!(
                "{kind} '{name}': property '{key}' is nested; connector properties must be flat (quote dotted keys such as \"bootstrap.servers\")"
            ));
        }
    }
}

fn connector_property_is_flat(value: &toml::Value) -> bool {
    match value {
        toml::Value::Table(_) => false,
        toml::Value::Array(values) => values.iter().all(connector_property_is_flat),
        _ => true,
    }
}
