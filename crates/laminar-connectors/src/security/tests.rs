use super::*;

#[test]
fn uri_secret_detection_covers_userinfo_lists_and_sas_signatures() {
    assert!(value_contains_uri_secret(
        "wss://public.test, wss://user:pass@private.test",
        true
    ));
    assert!(value_contains_uri_secret(
        "https://blob.test/file?sv=1&sig=signed",
        true
    ));
    assert!(!value_contains_uri_secret(
        "mongodb://user:${MONGO_PASSWORD}@db.test/data",
        true
    ));
}

#[test]
fn oauth_bearer_options_are_classified_without_provider_specific_callers() {
    assert!(is_secret_option_key("sasl.oauthbearer.config"));
    assert!(is_secret_option_key("oauthbearer-token"));
}

#[test]
fn durable_identity_preserves_endpoint_but_removes_uri_credentials() {
    let sanitized = sanitize_identity_value(
        "connection.uri",
        "mongodb://user:pass@db.test/data?replicaSet=rs0&token=abc \
         mongodb://next:secret@db2.test/data",
    );
    assert_eq!(
        sanitized,
        "mongodb://<redacted>@db.test/data?replicaSet=rs0&token=<redacted> \
         mongodb://<redacted>@db2.test/data"
    );
    assert!(!sanitized.contains("user"));
    assert!(!sanitized.contains("pass"));
    assert!(!sanitized.contains("abc"));
}
