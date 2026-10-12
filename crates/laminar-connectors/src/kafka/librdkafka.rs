//! Compiled librdkafka capabilities and the passthrough-property policy shared by the source and
//! sink.

use std::collections::HashMap;
use std::sync::OnceLock;

use rdkafka::ClientConfig;

use super::config::{SaslMechanism, SecurityProtocol};
use crate::error::ConnectorError;

/// librdkafka aliases, keyed by alias, that configure the same native property.
const PROPERTY_ALIASES: &[(&str, &str)] = &[
    ("bootstrap.servers", "metadata.broker.list"),
    ("max.in.flight", "max.in.flight.requests.per.connection"),
    ("sasl.mechanism", "sasl.mechanisms"),
    (
        "sasl.oauthbearer.client.credentials.client.id",
        "sasl.oauthbearer.client.id",
    ),
    (
        "sasl.oauthbearer.client.credentials.client.secret",
        "sasl.oauthbearer.client.secret",
    ),
    ("max.partition.fetch.bytes", "fetch.message.max.bytes"),
    ("linger.ms", "queue.buffering.max.ms"),
    ("retries", "message.send.max.retries"),
    ("compression.type", "compression.codec"),
    ("acks", "request.required.acks"),
    ("delivery.timeout.ms", "message.timeout.ms"),
    ("enable.auto.commit", "auto.commit.enable"),
];

/// Properties no `kafka.*` passthrough may set on any Kafka client, whether or not the connector
/// sets them itself: credentials and transport security have typed options, and the rest would
/// change delivery semantics behind the connector's validation.
const ALWAYS_PROTECTED: &[&str] = &[
    "metadata.broker.list",
    "security.protocol",
    "sasl.mechanisms",
    "sasl.username",
    "sasl.password",
    "ssl.ca.location",
    "ssl.ca.pem",
    "ssl.certificate.location",
    "ssl.certificate.pem",
    "ssl.key.location",
    "ssl.key.pem",
    "ssl.key.password",
    "ssl.keystore.location",
    "ssl.keystore.password",
    "ssl.endpoint.identification.algorithm",
    "enable.ssl.certificate.verification",
    "transactional.id",
    "transaction.timeout.ms",
    "enable.idempotence",
    "enable.gapless.guarantee",
    "auto.commit.enable",
    "enable.auto.offset.store",
    "auto.commit.interval.ms",
    "isolation.level",
    "group.protocol",
    "partitioner",
];

const PROTECTED_PREFIXES: &[&str] = &["sasl.kerberos.", "sasl.oauthbearer."];

fn canonical_property(key: &str) -> &str {
    PROPERTY_ALIASES
        .iter()
        .find_map(|(alias, canonical)| (*alias == key).then_some(*canonical))
        .unwrap_or(key)
}

/// Reject `kafka.*` passthrough properties that would override a connector-owned setting.
///
/// `owned` is the connector-built configuration before passthrough is applied. A passthrough key
/// is rejected when it, or any alias of it, names a property `owned` sets or one that is always
/// protected. Rejection happens during validation so the effective configuration a pipeline runs
/// with is exactly the one its options describe.
///
/// # Errors
/// Returns a configuration error naming the first conflicting key.
pub(crate) fn validate_passthrough(
    owned: &ClientConfig,
    passthrough: &HashMap<String, String>,
) -> Result<(), ConnectorError> {
    let owned_keys = owned
        .config_map()
        .into_keys()
        .map(canonical_property)
        .collect::<Vec<_>>();
    let mut keys = passthrough.keys().collect::<Vec<_>>();
    keys.sort_unstable();
    for key in keys {
        let canonical = canonical_property(key);
        let protected = ALWAYS_PROTECTED.contains(&canonical)
            || PROTECTED_PREFIXES
                .iter()
                .any(|prefix| canonical.starts_with(prefix));
        if protected || owned_keys.contains(&canonical) {
            return Err(ConnectorError::ConfigurationError(format!(
                "'kafka.{key}' overrides a setting the Kafka connector owns; use the connector \
                 option for it instead"
            )));
        }
    }
    Ok(())
}

/// Apply validated passthrough properties after the connector-owned settings.
pub(crate) fn apply_passthrough(config: &mut ClientConfig, passthrough: &HashMap<String, String>) {
    for (key, value) in passthrough {
        config.set(key, value);
    }
}

/// The broker address and transport-security settings of `config`, for auxiliary clients such as
/// the admin client.
#[must_use]
pub(crate) fn connection_subset(config: &ClientConfig) -> ClientConfig {
    let mut connection = ClientConfig::new();
    for (key, value) in config.config_map() {
        let canonical = canonical_property(key);
        if canonical == "metadata.broker.list"
            || key == "security.protocol"
            || key.starts_with("sasl.")
            || key.starts_with("ssl.")
        {
            connection.set(key, value);
        }
    }
    connection
}

/// Configuration rendered for logs: secrets are replaced by `***`.
#[must_use]
pub(crate) fn redacted_settings(config: &ClientConfig) -> Vec<(String, String)> {
    config
        .config_map()
        .into_iter()
        .map(|(key, value)| {
            let secret = key.contains("password")
                || key.contains("secret")
                || key.contains(".pem")
                || key == "sasl.oauthbearer.config";
            let value = if secret { "***" } else { value };
            (key.to_string(), value.to_string())
        })
        .collect()
}

/// Feature flags compiled into the linked librdkafka (`builtin.features`).
fn builtin_features() -> Result<&'static [String], ConnectorError> {
    static FEATURES: OnceLock<Result<Vec<String>, String>> = OnceLock::new();
    FEATURES
        .get_or_init(|| {
            ClientConfig::new()
                .create_native_config()
                .and_then(|native| native.get("builtin.features"))
                .map(|features| features.split(',').map(str::to_owned).collect())
                .map_err(|error| error.to_string())
        })
        .as_deref()
        .map_err(|error| {
            ConnectorError::Internal(format!("cannot read librdkafka builtin.features: {error}"))
        })
}

fn require_builtin(feature: &str, purpose: &str) -> Result<(), ConnectorError> {
    let built = builtin_features()?;
    if built.iter().any(|name| name == feature) {
        return Ok(());
    }
    Err(ConnectorError::ConfigurationError(format!(
        "{purpose} requires librdkafka feature '{feature}', which this build does not include \
         (built: {})",
        built.join(",")
    )))
}

/// Fail before client creation when a configured mechanism or codec is not compiled in, or has
/// no supported credential lifecycle in this connector.
///
/// # Errors
/// Returns a configuration error naming the unsupported option.
pub(crate) fn require_supported_client(
    security_protocol: SecurityProtocol,
    sasl_mechanism: Option<SaslMechanism>,
    compression_codec: Option<&str>,
) -> Result<(), ConnectorError> {
    if security_protocol.uses_ssl() {
        require_builtin("ssl", &format!("security.protocol={security_protocol}"))?;
    }
    match sasl_mechanism {
        None => {}
        Some(SaslMechanism::Plain) => require_builtin("sasl_plain", "sasl.mechanism=PLAIN")?,
        Some(SaslMechanism::ScramSha256 | SaslMechanism::ScramSha512) => {
            require_builtin("sasl_scram", "SCRAM authentication")?;
        }
        Some(SaslMechanism::Gssapi) => {
            require_builtin("sasl_gssapi", "sasl.mechanism=GSSAPI (Kerberos)")?;
        }
        Some(SaslMechanism::Oauthbearer) => {
            return Err(ConnectorError::ConfigurationError(
                "sasl.mechanism=OAUTHBEARER is unsupported: this connector has no OAuth token \
                 provider or refresh integration, and librdkafka is built without OIDC"
                    .into(),
            ));
        }
    }
    if let Some(codec) = compression_codec.filter(|codec| *codec != "none") {
        if matches!(codec, "gzip" | "snappy" | "lz4" | "zstd") {
            require_builtin(codec, &format!("compression.type={codec}"))?;
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn owned() -> ClientConfig {
        let mut config = ClientConfig::new();
        config
            .set("bootstrap.servers", "broker:9092")
            .set("linger.ms", "5")
            .set("acks", "all");
        config
    }

    fn passthrough(key: &str) -> HashMap<String, String> {
        HashMap::from([(key.to_string(), "1".to_string())])
    }

    #[test]
    fn passthrough_rejects_owned_keys_and_their_aliases() {
        for key in [
            "linger.ms",
            "queue.buffering.max.ms",
            "request.required.acks",
            "metadata.broker.list",
        ] {
            let error = validate_passthrough(&owned(), &passthrough(key)).unwrap_err();
            assert!(error.to_string().contains(key), "{key}: {error}");
        }
    }

    #[test]
    fn passthrough_rejects_protected_keys_even_when_unset() {
        for key in [
            "isolation.level",
            "transactional.id",
            "sasl.oauthbearer.config",
            "sasl.kerberos.keytab",
            "enable.auto.offset.store",
            "group.protocol",
        ] {
            assert!(
                validate_passthrough(&owned(), &passthrough(key)).is_err(),
                "{key}"
            );
        }
    }

    #[test]
    fn passthrough_accepts_unowned_tuning() {
        validate_passthrough(&owned(), &passthrough("socket.keepalive.enable")).unwrap();
        validate_passthrough(&owned(), &passthrough("fetch.error.backoff.ms")).unwrap();
    }

    #[test]
    fn redaction_hides_secrets() {
        let mut config = ClientConfig::new();
        config
            .set("sasl.password", "hunter2")
            .set("ssl.key.password", "hunter3")
            .set("linger.ms", "5");
        let rendered = redacted_settings(&config);
        assert!(rendered
            .iter()
            .all(|(_, value)| !value.starts_with("hunter")));
        assert!(rendered.contains(&("linger.ms".into(), "5".into())));
    }

    #[test]
    fn compiled_features_cover_the_shipped_options() {
        let tls = SecurityProtocol::SaslSsl;
        require_supported_client(tls, Some(SaslMechanism::Plain), Some("gzip")).unwrap();
        require_supported_client(tls, Some(SaslMechanism::ScramSha512), Some("lz4")).unwrap();
        assert!(require_supported_client(tls, Some(SaslMechanism::Oauthbearer), None).is_err());
        let built = |name: &str| builtin_features().unwrap().iter().any(|f| f == name);
        let gssapi = require_supported_client(tls, Some(SaslMechanism::Gssapi), None);
        assert_eq!(gssapi.is_ok(), built("sasl_gssapi"));
        for codec in ["snappy", "zstd"] {
            let supported = require_supported_client(SecurityProtocol::Ssl, None, Some(codec));
            assert_eq!(supported.is_ok(), built(codec), "{codec}");
        }
        assert_eq!(cfg!(feature = "kafka-zstd"), built("zstd"));
    }
}
