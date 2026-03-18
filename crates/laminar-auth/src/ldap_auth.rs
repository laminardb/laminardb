//! LDAP Authentication Provider.
//!
//! Authenticates users by performing an LDAP bind + search operation.
//! Group memberships are extracted and mapped to roles.
//!
//! # Configuration
//!
//! ```toml
//! [auth.ldap]
//! url = "ldap://ldap.example.com:389"
//! bind_dn = "cn=admin,dc=example,dc=com"
//! bind_password = "${LDAP_BIND_PASSWORD}"
//! search_base = "dc=example,dc=com"
//! user_filter = "(uid={username})"
//! group_attribute = "memberOf"
//! ```

use std::collections::HashMap;

use ldap3::{LdapConnAsync, Scope, SearchEntry};
use tracing::{debug, warn};

use crate::identity::{AuthError, AuthMethod, AuthProvider, Credentials, Identity};

/// LDAP authentication configuration.
#[derive(Debug, Clone)]
pub struct LdapConfig {
    /// LDAP server URL (e.g., `ldap://ldap.example.com:389`).
    pub url: String,
    /// Bind DN for the service account used to search users.
    pub bind_dn: String,
    /// Bind password for the service account.
    pub bind_password: String,
    /// Base DN for user searches.
    pub search_base: String,
    /// LDAP filter for finding users. `{username}` is replaced with the login name.
    pub user_filter: String,
    /// Attribute containing group memberships (e.g., `memberOf`).
    pub group_attribute: String,
    /// Optional mapping from LDAP group DNs to role names.
    pub group_role_mapping: HashMap<String, String>,
    /// Whether to use STARTTLS.
    pub starttls: bool,
}

impl Default for LdapConfig {
    fn default() -> Self {
        Self {
            url: "ldap://localhost:389".to_string(),
            bind_dn: String::new(),
            bind_password: String::new(),
            search_base: String::new(),
            user_filter: "(uid={username})".to_string(),
            group_attribute: "memberOf".to_string(),
            group_role_mapping: HashMap::new(),
            starttls: false,
        }
    }
}

/// LDAP authentication provider.
pub struct LdapAuthProvider {
    config: LdapConfig,
}

impl LdapAuthProvider {
    /// Create a new LDAP auth provider with the given configuration.
    pub fn new(config: LdapConfig) -> Self {
        Self { config }
    }
}

impl std::fmt::Debug for LdapAuthProvider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LdapAuthProvider")
            .field("url", &self.config.url)
            .field("search_base", &self.config.search_base)
            .finish()
    }
}

#[async_trait::async_trait]
impl AuthProvider for LdapAuthProvider {
    async fn authenticate(&self, credentials: &Credentials) -> Result<Identity, AuthError> {
        let (username, password) = match credentials {
            Credentials::UsernamePassword { username, password } => (username, password),
            _ => {
                return Err(AuthError::AuthenticationFailed(
                    "LDAP provider requires username/password credentials".to_string(),
                ));
            }
        };

        // 1. Connect with service account
        let (conn, mut ldap) = LdapConnAsync::new(&self.config.url)
            .await
            .map_err(|e| AuthError::ProviderUnavailable(format!("LDAP connect failed: {e}")))?;

        // Drive the connection in the background
        tokio::spawn(async move { conn.drive().await });

        // 2. Bind as service account
        ldap.simple_bind(&self.config.bind_dn, &self.config.bind_password)
            .await
            .map_err(|e| AuthError::ProviderUnavailable(format!("LDAP service bind failed: {e}")))?
            .success()
            .map_err(|e| {
                AuthError::ProviderUnavailable(format!("LDAP service bind rejected: {e}"))
            })?;

        debug!(username = %username, "LDAP: searching for user");

        // 3. Search for the user DN
        let filter = self.config.user_filter.replace("{username}", username);
        let (results, _) = ldap
            .search(
                &self.config.search_base,
                Scope::Subtree,
                &filter,
                vec!["dn", &self.config.group_attribute],
            )
            .await
            .map_err(|e| AuthError::ProviderUnavailable(format!("LDAP search failed: {e}")))?
            .success()
            .map_err(|e| AuthError::ProviderUnavailable(format!("LDAP search error: {e}")))?;

        if results.is_empty() {
            return Err(AuthError::AuthenticationFailed(format!(
                "user '{}' not found in LDAP",
                username
            )));
        }

        let entry = SearchEntry::construct(results.into_iter().next().unwrap());
        let user_dn = entry.dn;

        // 4. Bind as the user to verify password
        ldap.simple_bind(&user_dn, password)
            .await
            .map_err(|e| AuthError::AuthenticationFailed(format!("LDAP user bind failed: {e}")))?
            .success()
            .map_err(|_| {
                AuthError::AuthenticationFailed(format!("invalid password for user '{}'", username))
            })?;

        debug!(username = %username, dn = %user_dn, "LDAP: user authenticated");

        // 5. Extract groups and map to roles
        let mut identity = Identity::new(username.clone(), AuthMethod::Ldap);
        identity.attributes.insert("dn".to_string(), user_dn);

        if let Some(groups) = entry.attrs.get(&self.config.group_attribute) {
            for group_dn in groups {
                // Try mapping group DN to role name
                if let Some(role) = self.config.group_role_mapping.get(group_dn) {
                    identity.roles.insert(role.clone());
                } else {
                    // Extract CN from group DN as fallback role name
                    if let Some(cn) = extract_cn(group_dn) {
                        identity.roles.insert(cn);
                    } else {
                        warn!(group = %group_dn, "LDAP: could not extract CN from group DN");
                    }
                }
            }
        }

        let _ = ldap.unbind().await;

        Ok(identity)
    }

    fn name(&self) -> &str {
        "ldap"
    }
}

/// Extract the CN value from a distinguished name string.
///
/// `"cn=engineers,ou=groups,dc=example,dc=com"` → `"engineers"`
fn extract_cn(dn: &str) -> Option<String> {
    for part in dn.split(',') {
        let part = part.trim();
        if let Some(cn) = part
            .strip_prefix("cn=")
            .or_else(|| part.strip_prefix("CN="))
        {
            return Some(cn.to_string());
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_extract_cn() {
        assert_eq!(
            extract_cn("cn=engineers,ou=groups,dc=example,dc=com"),
            Some("engineers".to_string())
        );
        assert_eq!(
            extract_cn("CN=Admins,OU=Groups,DC=corp,DC=com"),
            Some("Admins".to_string())
        );
        assert_eq!(extract_cn("ou=groups,dc=com"), None);
    }

    #[test]
    fn test_ldap_config_defaults() {
        let config = LdapConfig::default();
        assert_eq!(config.url, "ldap://localhost:389");
        assert_eq!(config.user_filter, "(uid={username})");
        assert_eq!(config.group_attribute, "memberOf");
        assert!(!config.starttls);
    }

    #[test]
    fn test_ldap_provider_name() {
        let provider = LdapAuthProvider::new(LdapConfig::default());
        assert_eq!(provider.name(), "ldap");
    }

    #[tokio::test]
    async fn test_ldap_rejects_non_password_credentials() {
        let provider = LdapAuthProvider::new(LdapConfig::default());
        let result = provider
            .authenticate(&Credentials::BearerToken("token".to_string()))
            .await;
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("username/password"));
    }
}
