//! Verified TLS policy and client construction shared by `PostgreSQL` connectors.

use std::path::Path;
use std::str::FromStr;

use tokio_postgres_rustls::MakeRustlsConnect;
use tokio_rustls::rustls::pki_types::{pem::PemObject, CertificateDer};
use tokio_rustls::rustls::{ClientConfig, RootCertStore};

use crate::error::ConnectorError;

/// Connection security for `PostgreSQL` connectors.
///
/// Production connections either verify both the certificate chain and server
/// hostname, or explicitly opt into plaintext for trusted test networks.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum SslMode {
    /// Disable TLS explicitly.
    Disable,
    /// Require TLS with certificate-chain and hostname verification.
    #[default]
    VerifyFull,
}

impl std::fmt::Display for SslMode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Disable => write!(f, "disable"),
            Self::VerifyFull => write!(f, "verify-full"),
        }
    }
}

impl FromStr for SslMode {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_ascii_lowercase().as_str() {
            "disable" => Ok(Self::Disable),
            "verify-full" => Ok(Self::VerifyFull),
            other => Err(format!("unknown SSL mode: '{other}'")),
        }
    }
}

pub(crate) fn make_rustls_connector(
    ca_cert_path: Option<&Path>,
) -> Result<MakeRustlsConnect, ConnectorError> {
    // The workspace standardizes on aws-lc-rs. Explicit installation also avoids
    // rustls provider ambiguity when pgwire-replication enables its ring feature.
    let _ = tokio_rustls::rustls::crypto::aws_lc_rs::default_provider().install_default();

    let mut roots = RootCertStore::empty();
    if let Some(path) = ca_cert_path {
        let certificates = CertificateDer::pem_file_iter(path)
            .map_err(|error| {
                ConnectorError::ConfigurationError(format!(
                    "read PostgreSQL CA certificate '{}': {error}",
                    path.display()
                ))
            })?
            .collect::<Result<Vec<_>, _>>()
            .map_err(|error| {
                ConnectorError::ConfigurationError(format!(
                    "parse PostgreSQL CA certificate '{}': {error}",
                    path.display()
                ))
            })?;
        if certificates.is_empty() {
            return Err(ConnectorError::ConfigurationError(format!(
                "PostgreSQL CA certificate '{}' contains no certificates",
                path.display()
            )));
        }
        for certificate in certificates {
            roots.add(certificate).map_err(|error| {
                ConnectorError::ConfigurationError(format!(
                    "invalid PostgreSQL CA certificate '{}': {error}",
                    path.display()
                ))
            })?;
        }
    } else {
        roots.extend(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());
    }

    let config = ClientConfig::builder()
        .with_root_certificates(roots)
        .with_no_client_auth();
    Ok(MakeRustlsConnect::new(config))
}

#[cfg(test)]
mod tests {
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
}
