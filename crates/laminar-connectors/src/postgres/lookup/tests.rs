use super::*;
use arrow_array::{Int64Array, StringArray};

#[test]
fn pg_type_map_native_and_explicit_text_projection() {
    assert_eq!(pg_type_to_arrow(&Type::INT8), DataType::Int64);
    assert_eq!(pg_type_to_arrow(&Type::FLOAT8), DataType::Float64);
    assert_eq!(pg_type_to_arrow(&Type::BOOL), DataType::Boolean);
    assert_eq!(
        pg_type_to_arrow(&Type::TIMESTAMP),
        DataType::Timestamp(TimeUnit::Microsecond, None)
    );
    assert_eq!(pg_type_to_arrow(&Type::NUMERIC), DataType::Utf8);
    assert_eq!(pg_type_to_arrow(&Type::UUID), DataType::Utf8);
    assert_eq!(
        select_expression_for("amount", &Type::NUMERIC).unwrap(),
        "CAST(\"amount\" AS TEXT) AS \"amount\""
    );
    assert_eq!(
        select_expression_for("created_at", &Type::TIMESTAMP).unwrap(),
        "\"created_at\""
    );
    assert!(!supports_any_parameter(&Type::UUID));
    assert!(supports_any_parameter(&Type::INT8));
}

#[test]
fn unique_key_catalog_probe_is_fail_closed_and_allows_include_columns() {
    for required in [
        "pg_catalog.to_regclass($1)",
        "idx.indisunique",
        "idx.indisvalid",
        "idx.indisready",
        "idx.indislive",
        "idx.indnkeyatts = 1",
        "idx.indpred IS NULL",
        "idx.indexprs IS NULL",
        "attr.attnum = idx.indkey[0]",
        "attr.attname = $2",
    ] {
        assert!(
            UNIQUE_LOOKUP_KEY_QUERY.contains(required),
            "missing {required}"
        );
    }
    assert!(
        !UNIQUE_LOOKUP_KEY_QUERY.contains("indnatts = 1"),
        "included columns must not invalidate a single-key unique index"
    );

    assert!(validate_unique_lookup_key("events", "id", None, false).is_err());
    assert!(validate_unique_lookup_key("events", "id", Some(42), false).is_err());
    assert_eq!(
        validate_unique_lookup_key("events", "id", Some(42), true).unwrap(),
        42
    );
}

#[test]
fn any_param_built_for_supported_types_skipping_nulls() {
    assert!(
        PostgresLookupSource::build_any_param(&Int64Array::from(vec![Some(1i64), None, Some(3)]))
            .is_ok()
    );
    assert!(PostgresLookupSource::build_any_param(&StringArray::from(vec!["a", "b"])).is_ok());
}

#[test]
fn any_param_rejects_unsupported_type() {
    assert!(
        PostgresLookupSource::build_any_param(&arrow_array::Date32Array::from(vec![1])).is_err()
    );
}

fn props(kv: &[(&str, &str)]) -> HashMap<String, String> {
    kv.iter().map(|(k, v)| ((*k).into(), (*v).into())).collect()
}

#[test]
fn tls_mode_parsing() {
    assert_eq!(
        ssl_mode(&HashMap::new()).unwrap(),
        crate::postgres::SslMode::VerifyFull
    );
    assert_eq!(
        ssl_mode(&props(&[("ssl.mode", "disable")])).unwrap(),
        crate::postgres::SslMode::Disable
    );
    assert_eq!(
        ssl_mode(&props(&[("ssl.mode", "verify-full")])).unwrap(),
        crate::postgres::SslMode::VerifyFull
    );
    assert_eq!(
        driver_ssl_mode(crate::postgres::SslMode::VerifyFull),
        deadpool_postgres::SslMode::Require
    );
    assert_eq!(
        driver_ssl_mode(crate::postgres::SslMode::Disable),
        deadpool_postgres::SslMode::Disable
    );
    for rejected in ["prefer", "require", "verify-ca", "bogus"] {
        assert!(ssl_mode(&props(&[("ssl.mode", rejected)])).is_err());
    }
    assert!(ssl_mode(&props(&[("sslmode", "disable")])).is_err());
    assert!(ssl_mode(&props(&[
        ("ssl.mode", "disable"),
        ("ssl.ca.cert.path", "/certs/ca.pem"),
    ]))
    .is_err());
}

#[test]
fn lookup_key_admission_is_bounded() {
    assert!(validate_lookup_keys(&[&b"a"[..], &b"bc"[..]]).is_ok());

    let too_many = vec![&b""[..]; MAX_LOOKUP_KEYS + 1];
    assert!(validate_lookup_keys(&too_many).is_err());

    let oversized = vec![0_u8; MAX_LOOKUP_KEY_BYTES + 1];
    assert!(validate_lookup_keys(&[oversized.as_slice()]).is_err());
}

#[test]
fn pool_configuration_rejects_invalid_values() {
    let base = [
        ("host", "localhost"),
        ("database", "db"),
        ("user", "user"),
        ("ssl.mode", "disable"),
    ];
    assert!(build_pool(&props(&base), 0).is_err());
    assert!(build_pool(&props(&base), MAX_POOL_SIZE + 1).is_err());

    let mut invalid_port = props(&base);
    invalid_port.insert("port".into(), "not-a-port".into());
    assert!(build_pool(&invalid_port, 1).is_err());
    invalid_port.insert("port".into(), "0".into());
    assert!(build_pool(&invalid_port, 1).is_err());

    let mut invalid_options = props(&base);
    invalid_options.insert("options".into(), "bad\0option".into());
    assert!(build_pool(&invalid_options, 1).is_err());

    let mut empty_user = props(&base);
    empty_user.insert("user".into(), " ".into());
    assert!(build_pool(&empty_user, 1).is_err());

    let conflict = props(&[
        ("connection", "host=localhost dbname=db user=user"),
        ("host", "other"),
        ("ssl.mode", "disable"),
    ]);
    assert!(build_pool(&conflict, 1).is_err());

    let zero_port = props(&[
        ("connection", "host=localhost port=0 dbname=db user=user"),
        ("ssl.mode", "disable"),
    ]);
    assert!(build_pool(&zero_port, 1).is_err());
    assert!(build_pool(&props(&[("connection", ""), ("ssl.mode", "disable")]), 1).is_err());
}

#[test]
fn identifier_validation_rejects_unsafe_shapes() {
    assert_eq!(
        quote_qualified_identifier("public.events").unwrap(),
        "\"public\".\"events\""
    );
    assert!(quote_qualified_identifier("public.").is_err());
    assert!(quote_identifier("bad\0name").is_err());
}

#[test]
fn tls_connector_builds_with_roots_and_rejects_bad_ca() {
    // Default webpki roots: builds without a CA file.
    assert!(build_rustls_connector(&HashMap::new()).is_ok());
    // An explicit but missing CA path is a clear error, not a panic.
    assert!(build_rustls_connector(&props(&[("ssl.ca.cert.path", "/no/such/ca.pem")])).is_err());
}
