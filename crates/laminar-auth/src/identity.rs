//! Core identity types, authentication provider trait, and errors.
//!
//! [`Identity`] is the central type that flows through the entire auth pipeline:
//! authentication providers produce it, authorization policies consume it.

use std::collections::{HashMap, HashSet};

/// A verified identity produced by an [`AuthProvider`].
///
/// Carries the principal name, assigned roles, and arbitrary attributes
/// that ABAC and row/column security policies can evaluate.
#[derive(Debug, Clone)]
pub struct Identity {
    /// Principal name (e.g., username, certificate CN, service account).
    pub name: String,

    /// Authentication method that produced this identity.
    pub auth_method: AuthMethod,

    /// Roles assigned to this identity (used by RBAC).
    pub roles: HashSet<String>,

    /// Arbitrary key-value attributes (used by ABAC).
    ///
    /// Examples: `department=engineering`, `tenant_id=acme`, `clearance=3`.
    pub attributes: HashMap<String, String>,
}

impl Identity {
    /// Create a new identity with the given name and auth method.
    pub fn new(name: impl Into<String>, auth_method: AuthMethod) -> Self {
        Self {
            name: name.into(),
            auth_method,
            roles: HashSet::new(),
            attributes: HashMap::new(),
        }
    }

    /// Add a role to this identity.
    pub fn with_role(mut self, role: impl Into<String>) -> Self {
        self.roles.insert(role.into());
        self
    }

    /// Add an attribute to this identity.
    pub fn with_attribute(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.attributes.insert(key.into(), value.into());
        self
    }

    /// Check whether this identity has a specific role.
    pub fn has_role(&self, role: &str) -> bool {
        self.roles.contains(role)
    }

    /// Check whether the identity has the superuser role.
    pub fn is_superuser(&self) -> bool {
        self.roles.contains("superuser")
    }
}

/// How the identity was authenticated.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AuthMethod {
    /// Mutual TLS client certificate.
    MtlsCertificate,
    /// LDAP bind authentication.
    Ldap,
    /// JWT bearer token.
    Jwt,
    /// No authentication (anonymous / trusted network).
    Anonymous,
}

impl std::fmt::Display for AuthMethod {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::MtlsCertificate => write!(f, "mtls"),
            Self::Ldap => write!(f, "ldap"),
            Self::Jwt => write!(f, "jwt"),
            Self::Anonymous => write!(f, "anonymous"),
        }
    }
}

/// Trait for authentication providers.
///
/// Each provider extracts an [`Identity`] from some credential material
/// (certificate, LDAP bind, JWT token, etc.).
#[async_trait::async_trait]
pub trait AuthProvider: Send + Sync {
    /// Attempt to authenticate using the given credentials.
    ///
    /// Returns an [`Identity`] on success or [`AuthError`] on failure.
    async fn authenticate(&self, credentials: &Credentials) -> Result<Identity, AuthError>;

    /// Human-readable name for this provider (e.g., "mtls", "ldap").
    fn name(&self) -> &str;
}

/// Credential material presented by a client.
#[derive(Debug, Clone)]
pub enum Credentials {
    /// Raw DER-encoded client certificate bytes.
    Certificate(Vec<u8>),

    /// Username + password (for LDAP bind).
    UsernamePassword {
        /// Username or distinguished name.
        username: String,
        /// Password.
        password: String,
    },

    /// Bearer token (JWT).
    BearerToken(String),

    /// No credentials (anonymous access).
    Anonymous,
}

/// Authentication and authorization errors.
#[derive(Debug, thiserror::Error)]
pub enum AuthError {
    /// Invalid or expired credentials.
    #[error("authentication failed: {0}")]
    AuthenticationFailed(String),

    /// Credentials are valid but insufficient for the requested operation.
    #[error("access denied: {0}")]
    AccessDenied(String),

    /// The auth provider is unavailable (e.g., LDAP server down).
    #[error("auth provider unavailable: {0}")]
    ProviderUnavailable(String),

    /// Configuration error in the auth subsystem.
    #[error("auth configuration error: {0}")]
    Configuration(String),

    /// Internal error.
    #[error("internal auth error: {0}")]
    Internal(String),
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_identity_builder() {
        let id = Identity::new("alice", AuthMethod::Ldap)
            .with_role("analyst")
            .with_role("viewer")
            .with_attribute("department", "engineering")
            .with_attribute("tenant_id", "acme");

        assert_eq!(id.name, "alice");
        assert_eq!(id.auth_method, AuthMethod::Ldap);
        assert!(id.has_role("analyst"));
        assert!(id.has_role("viewer"));
        assert!(!id.has_role("admin"));
        assert!(!id.is_superuser());
        assert_eq!(id.attributes.get("department").unwrap(), "engineering");
        assert_eq!(id.attributes.get("tenant_id").unwrap(), "acme");
    }

    #[test]
    fn test_superuser_role() {
        let id = Identity::new("root", AuthMethod::Anonymous).with_role("superuser");
        assert!(id.is_superuser());
    }

    #[test]
    fn test_auth_method_display() {
        assert_eq!(AuthMethod::MtlsCertificate.to_string(), "mtls");
        assert_eq!(AuthMethod::Ldap.to_string(), "ldap");
        assert_eq!(AuthMethod::Jwt.to_string(), "jwt");
        assert_eq!(AuthMethod::Anonymous.to_string(), "anonymous");
    }
}
