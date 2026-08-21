use super::*;

#[test]
fn ssl_mode_defaults_to_verified_tls() {
    assert_eq!(SslMode::default(), SslMode::VerifyFull);
}

#[test]
fn ssl_mode_parses_only_supported_policies() {
    assert_eq!("disable".parse::<SslMode>().unwrap(), SslMode::Disable);
    assert_eq!(
        "verify-full".parse::<SslMode>().unwrap(),
        SslMode::VerifyFull
    );
    assert_eq!(
        "VERIFY-FULL".parse::<SslMode>().unwrap(),
        SslMode::VerifyFull
    );

    for rejected in [
        "allow",
        "off",
        "prefer",
        "require",
        "verify-ca",
        "verify_full",
        "verifyfull",
        "",
    ] {
        assert!(rejected.parse::<SslMode>().is_err(), "{rejected}");
    }
}

#[test]
fn ssl_mode_has_canonical_display_values() {
    assert_eq!(SslMode::Disable.to_string(), "disable");
    assert_eq!(SslMode::VerifyFull.to_string(), "verify-full");
}

#[test]
fn webpki_roots_build_without_filesystem_configuration() {
    make_rustls_connector(None).unwrap();
}

#[test]
fn malformed_custom_ca_fails_before_network_io() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("ca.pem");
    std::fs::write(&path, "not a certificate").unwrap();

    let error = make_rustls_connector(Some(&path))
        .err()
        .expect("malformed CA must fail");
    assert!(error.to_string().contains("PostgreSQL CA certificate"));
}
