//! Core authentication types: [`Identity`], [`AuthProvider`], [`Credentials`].
//!
//! The [`AuthProvider`] trait defines the contract for pluggable authentication
//! backends (JWT, mTLS, LDAP, etc.). Each backend validates credentials and
//! returns an [`Identity`] on success.

use std::collections::{HashMap, HashSet};

use serde::{Deserialize, Serialize};

use crate::rbac::Role;

/// A verified user identity, produced by an [`AuthProvider`].
///
/// Contains the user's ID, assigned roles, arbitrary attributes (e.g., tenant
/// ID, department), and an optional expiry timestamp.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Identity {
    /// Unique user identifier (e.g., username, email, UUID).
    pub user_id: String,

    /// Roles assigned to this identity (used by RBAC).
    pub roles: HashSet<Role>,

    /// Arbitrary key-value attributes extracted from the auth token
    /// or directory (e.g., `tenant_id`, `department`).
    pub attributes: HashMap<String, String>,

    /// When this identity expires (UTC epoch seconds). `None` = no expiry.
    pub expires_at: Option<i64>,
}

impl Identity {
    /// Create a new identity with no roles, attributes, or expiry.
    pub fn new(user_id: impl Into<String>) -> Self {
        Self {
            user_id: user_id.into(),
            roles: HashSet::new(),
            attributes: HashMap::new(),
            expires_at: None,
        }
    }

    /// Add a role to this identity.
    #[must_use]
    pub fn with_role(mut self, role: Role) -> Self {
        self.roles.insert(role);
        self
    }

    /// Add an attribute to this identity.
    #[must_use]
    pub fn with_attribute(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.attributes.insert(key.into(), value.into());
        self
    }

    /// Set the expiry timestamp (UTC epoch seconds).
    #[must_use]
    pub fn with_expiry(mut self, epoch_secs: i64) -> Self {
        self.expires_at = Some(epoch_secs);
        self
    }

    /// Check whether this identity has expired.
    pub fn is_expired(&self) -> bool {
        if let Some(exp) = self.expires_at {
            let now = chrono::Utc::now().timestamp();
            now >= exp
        } else {
            false
        }
    }

    /// Check whether this identity has a specific role.
    pub fn has_role(&self, role: &Role) -> bool {
        self.roles.contains(role)
    }
}

/// Credentials presented by a client for authentication.
#[derive(Debug, Clone)]
pub enum Credentials {
    /// Bearer token (typically JWT).
    Bearer(String),

    /// Username and password (for basic auth or LDAP).
    Basic {
        /// Username.
        username: String,
        /// Password.
        password: String,
    },
}

/// Trait for authentication providers.
///
/// Implementations validate [`Credentials`] and return an [`Identity`] on
/// success. The provider is responsible for token verification, signature
/// validation, expiry checks, and role extraction.
#[async_trait::async_trait]
pub trait AuthProvider: Send + Sync {
    /// Authenticate the given credentials.
    ///
    /// Returns `Ok(Identity)` on success, or `Err(AuthError)` if
    /// authentication fails.
    async fn authenticate(&self, credentials: &Credentials) -> Result<Identity, AuthError>;

    /// Human-readable name for this provider (e.g., "jwt", "ldap").
    fn name(&self) -> &'static str;
}

/// Authentication errors.
#[derive(Debug, thiserror::Error)]
pub enum AuthError {
    /// Credentials are invalid (wrong password, expired token, bad signature).
    #[error("invalid credentials: {0}")]
    InvalidCredentials(String),

    /// The authentication provider is misconfigured.
    #[error("auth provider configuration error: {0}")]
    Configuration(String),

    /// The identity has expired.
    #[error("identity expired")]
    Expired,

    /// Internal error in the auth provider.
    #[error("internal auth error: {0}")]
    Internal(String),
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_identity_builder() {
        let id = Identity::new("alice")
            .with_role(Role::Admin)
            .with_role(Role::ReadWrite)
            .with_attribute("tenant", "acme")
            .with_expiry(9_999_999_999);

        assert_eq!(id.user_id, "alice");
        assert!(id.has_role(&Role::Admin));
        assert!(id.has_role(&Role::ReadWrite));
        assert!(!id.has_role(&Role::ReadOnly));
        assert_eq!(id.attributes.get("tenant").unwrap(), "acme");
        assert!(!id.is_expired());
    }

    #[test]
    fn test_identity_expired() {
        let id = Identity::new("bob").with_expiry(0); // epoch 0 = long expired
        assert!(id.is_expired());
    }

    #[test]
    fn test_identity_no_expiry_never_expires() {
        let id = Identity::new("carol");
        assert!(!id.is_expired());
    }

    #[test]
    fn test_credentials_variants() {
        let bearer = Credentials::Bearer("token123".to_string());
        let basic = Credentials::Basic {
            username: "user".to_string(),
            password: "pass".to_string(),
        };

        match bearer {
            Credentials::Bearer(t) => assert_eq!(t, "token123"),
            Credentials::Basic { .. } => panic!("expected Bearer"),
        }
        match basic {
            Credentials::Basic { username, password } => {
                assert_eq!(username, "user");
                assert_eq!(password, "pass");
            }
            Credentials::Bearer(_) => panic!("expected Basic"),
        }
    }
}
