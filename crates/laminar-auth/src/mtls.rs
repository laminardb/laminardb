//! mTLS Authentication Provider.
//!
//! Extracts client identity from a TLS client certificate. The Common Name
//! (CN) becomes the principal name, and Subject Alternative Names (SANs)
//! are added as attributes.
//!
//! # Configuration
//!
//! ```toml
//! [server.tls]
//! cert = "/path/to/server.crt"
//! key = "/path/to/server.key"
//! client_ca = "/path/to/ca.crt"  # enables mTLS
//! ```

use std::path::{Path, PathBuf};
use std::sync::Arc;

use tokio_rustls::rustls;
use tracing::debug;

use crate::identity::{AuthError, AuthMethod, AuthProvider, Credentials, Identity};

/// Configuration for TLS / mTLS.
#[derive(Debug, Clone)]
pub struct TlsConfig {
    /// Path to the server certificate (PEM).
    pub cert_path: PathBuf,
    /// Path to the server private key (PEM).
    pub key_path: PathBuf,
    /// Path to the client CA certificate (PEM). Enables mTLS when set.
    pub client_ca_path: Option<PathBuf>,
}

/// mTLS authentication provider.
///
/// Extracts identity from DER-encoded client certificates.
#[derive(Debug)]
pub struct MtlsAuthProvider {
    /// Roles to automatically assign to mTLS-authenticated identities.
    default_roles: Vec<String>,
}

impl MtlsAuthProvider {
    /// Create a new mTLS auth provider.
    pub fn new() -> Self {
        Self {
            default_roles: Vec::new(),
        }
    }

    /// Add a default role assigned to all mTLS-authenticated identities.
    pub fn with_default_role(mut self, role: impl Into<String>) -> Self {
        self.default_roles.push(role.into());
        self
    }
}

impl Default for MtlsAuthProvider {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait::async_trait]
impl AuthProvider for MtlsAuthProvider {
    async fn authenticate(&self, credentials: &Credentials) -> Result<Identity, AuthError> {
        let cert_der = match credentials {
            Credentials::Certificate(der) => der,
            _ => {
                return Err(AuthError::AuthenticationFailed(
                    "mTLS provider requires certificate credentials".to_string(),
                ));
            }
        };

        let (_, cert) = x509_parser::parse_x509_certificate(cert_der).map_err(|e| {
            AuthError::AuthenticationFailed(format!("failed to parse client certificate: {e}"))
        })?;

        // Extract CN from subject
        let cn = cert
            .subject()
            .iter_common_name()
            .next()
            .and_then(|cn| cn.as_str().ok())
            .ok_or_else(|| {
                AuthError::AuthenticationFailed(
                    "client certificate has no Common Name (CN)".to_string(),
                )
            })?;

        debug!(cn = %cn, "mTLS: extracted identity from client certificate");

        let mut identity = Identity::new(cn, AuthMethod::MtlsCertificate);

        // Add default roles
        for role in &self.default_roles {
            identity.roles.insert(role.clone());
        }

        // Extract SANs as attributes
        if let Some(sans) = cert.subject_alternative_name().ok().flatten() {
            for san in &sans.value.general_names {
                match san {
                    x509_parser::extensions::GeneralName::DNSName(dns) => {
                        identity
                            .attributes
                            .insert("san_dns".to_string(), dns.to_string());
                    }
                    x509_parser::extensions::GeneralName::RFC822Name(email) => {
                        identity
                            .attributes
                            .insert("san_email".to_string(), email.to_string());
                    }
                    x509_parser::extensions::GeneralName::URI(uri) => {
                        identity
                            .attributes
                            .insert("san_uri".to_string(), uri.to_string());
                    }
                    _ => {}
                }
            }
        }

        // Add certificate fingerprint as attribute
        let fingerprint = {
            use sha2::Digest;
            let hash = sha2::Sha256::digest(cert_der);
            hex_encode(&hash)
        };
        identity
            .attributes
            .insert("cert_fingerprint".to_string(), fingerprint);

        Ok(identity)
    }

    fn name(&self) -> &str {
        "mtls"
    }
}

/// Build a rustls `ServerConfig` from the TLS configuration.
///
/// If `client_ca_path` is set, client certificate verification is required (mTLS).
pub fn build_tls_config(config: &TlsConfig) -> Result<Arc<rustls::ServerConfig>, AuthError> {
    let cert_pem = std::fs::read(&config.cert_path).map_err(|e| {
        AuthError::Configuration(format!(
            "failed to read server cert '{}': {}",
            config.cert_path.display(),
            e
        ))
    })?;
    let key_pem = std::fs::read(&config.key_path).map_err(|e| {
        AuthError::Configuration(format!(
            "failed to read server key '{}': {}",
            config.key_path.display(),
            e
        ))
    })?;

    let certs = rustls_pemfile::certs(&mut &cert_pem[..])
        .collect::<Result<Vec<_>, _>>()
        .map_err(|e| AuthError::Configuration(format!("failed to parse server cert: {e}")))?;

    let key = rustls_pemfile::private_key(&mut &key_pem[..])
        .map_err(|e| AuthError::Configuration(format!("failed to parse server key: {e}")))?
        .ok_or_else(|| AuthError::Configuration("no private key found in PEM file".to_string()))?;

    let builder = rustls::ServerConfig::builder();

    let server_config = if let Some(ca_path) = &config.client_ca_path {
        // mTLS: require client certificate
        let ca_pem = std::fs::read(ca_path).map_err(|e| {
            AuthError::Configuration(format!(
                "failed to read client CA '{}': {}",
                ca_path.display(),
                e
            ))
        })?;

        let mut root_store = rustls::RootCertStore::empty();
        let ca_certs = rustls_pemfile::certs(&mut &ca_pem[..])
            .collect::<Result<Vec<_>, _>>()
            .map_err(|e| AuthError::Configuration(format!("failed to parse client CA: {e}")))?;

        for cert in ca_certs {
            root_store.add(cert).map_err(|e| {
                AuthError::Configuration(format!("failed to add CA cert to root store: {e}"))
            })?;
        }

        let verifier = rustls::server::WebPkiClientVerifier::builder(Arc::new(root_store))
            .build()
            .map_err(|e| {
                AuthError::Configuration(format!("failed to build client verifier: {e}"))
            })?;

        builder
            .with_client_cert_verifier(verifier)
            .with_single_cert(certs, key)
            .map_err(|e| AuthError::Configuration(format!("failed to build TLS config: {e}")))?
    } else {
        // TLS only (no client cert verification)
        builder
            .with_no_client_auth()
            .with_single_cert(certs, key)
            .map_err(|e| AuthError::Configuration(format!("failed to build TLS config: {e}")))?
    };

    Ok(Arc::new(server_config))
}

/// Check if a TLS configuration path exists and is readable.
pub fn validate_tls_paths(config: &TlsConfig) -> Result<(), AuthError> {
    validate_file_readable(&config.cert_path, "server certificate")?;
    validate_file_readable(&config.key_path, "server key")?;
    if let Some(ca) = &config.client_ca_path {
        validate_file_readable(ca, "client CA certificate")?;
    }
    Ok(())
}

fn validate_file_readable(path: &Path, description: &str) -> Result<(), AuthError> {
    if !path.exists() {
        return Err(AuthError::Configuration(format!(
            "{description} not found: '{}'",
            path.display()
        )));
    }
    if !path.is_file() {
        return Err(AuthError::Configuration(format!(
            "{description} is not a file: '{}'",
            path.display()
        )));
    }
    Ok(())
}

fn hex_encode(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_mtls_provider_name() {
        let provider = MtlsAuthProvider::new();
        assert_eq!(provider.name(), "mtls");
    }

    #[tokio::test]
    async fn test_mtls_rejects_non_certificate_credentials() {
        let provider = MtlsAuthProvider::new();
        let result = provider
            .authenticate(&Credentials::BearerToken("token".to_string()))
            .await;
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("requires certificate"));
    }

    #[tokio::test]
    async fn test_mtls_rejects_invalid_certificate() {
        let provider = MtlsAuthProvider::new();
        let result = provider
            .authenticate(&Credentials::Certificate(vec![0, 1, 2, 3]))
            .await;
        assert!(result.is_err());
    }

    #[test]
    fn test_default_roles() {
        let provider = MtlsAuthProvider::new()
            .with_default_role("authenticated")
            .with_default_role("mtls-user");
        assert_eq!(provider.default_roles.len(), 2);
    }

    #[test]
    fn test_validate_tls_paths_missing() {
        let config = TlsConfig {
            cert_path: PathBuf::from("/nonexistent/cert.pem"),
            key_path: PathBuf::from("/nonexistent/key.pem"),
            client_ca_path: None,
        };
        assert!(validate_tls_paths(&config).is_err());
    }

    #[test]
    fn test_hex_encode() {
        assert_eq!(hex_encode(&[0x00, 0xFF, 0x42]), "00ff42");
    }
}
