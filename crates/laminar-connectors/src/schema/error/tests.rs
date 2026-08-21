use super::*;

#[test]
fn test_schema_error_display() {
    let err = SchemaError::InferenceFailed("too few samples".into());
    assert_eq!(err.to_string(), "inference failed: too few samples");
}

#[test]
fn test_schema_error_invalid_config() {
    let err = SchemaError::InvalidConfig {
        key: "format".into(),
        message: "unknown format 'xml'".into(),
    };
    assert!(err.to_string().contains("format"));
    assert!(err.to_string().contains("unknown format"));
}

#[test]
fn test_connector_to_schema_error() {
    let ce = ConnectorError::missing_config("topic");
    let se: SchemaError = ce.into();
    // `missing_config` lands in `ConfigurationError`, which maps to
    // `SchemaError::InvalidConfig` with the full message preserved.
    assert!(matches!(&se, SchemaError::InvalidConfig { message, .. } if message.contains("topic")));
}

#[test]
fn test_schema_to_connector_error() {
    let se = SchemaError::Incompatible("field type mismatch".into());
    let ce: ConnectorError = se.into();
    assert!(matches!(ce, ConnectorError::SchemaMismatch(_)));
}

#[test]
fn invalid_schema_config_remains_a_connector_configuration_error() {
    let error: ConnectorError = SchemaError::InvalidConfig {
        key: "json.column.ts.epoch_unit".into(),
        message: "invalid epoch unit".into(),
    }
    .into();
    assert!(matches!(error, ConnectorError::ConfigurationError(_)));
}

#[test]
fn test_schema_error_from_arrow() {
    let arrow_err = arrow_schema::ArrowError::SchemaError("bad schema".into());
    let se: SchemaError = arrow_err.into();
    assert!(matches!(se, SchemaError::Arrow(_)));
    assert!(se.to_string().contains("bad schema"));
}

#[test]
fn test_other_connector_error_wraps() {
    let ce = ConnectorError::ConnectionFailed("host down".into());
    let se: SchemaError = ce.into();
    assert!(matches!(se, SchemaError::Other(_)));
    assert!(se.to_string().contains("host down"));
}

#[test]
fn test_wildcard_errors_display() {
    let e1 = SchemaError::DuplicateWildcard;
    assert!(e1.to_string().contains("duplicate wildcard"));

    let e2 = SchemaError::WildcardWithoutResolution;
    assert!(e2.to_string().contains("schema provider"));

    let e3 = SchemaError::WildcardPrefixCollision("src_id".into());
    assert!(e3.to_string().contains("src_id"));

    let e4 = SchemaError::WildcardNoNewFields;
    assert!(e4.to_string().contains("zero new columns"));
}
