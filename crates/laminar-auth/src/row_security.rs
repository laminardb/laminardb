//! Row-Level Security (RLS).
//!
//! Provides per-table row-filtering predicates based on user identity.
//! When active, queries against a protected table automatically have a
//! `WHERE` predicate injected, restricting which rows are visible.
//!
//! # Example
//!
//! A policy on table `orders`:
//! ```text
//! user_attribute("tenant_id") = column("tenant_id")
//! ```
//! For a user with `tenant_id = "acme"`, this injects:
//! ```sql
//! WHERE tenant_id = 'acme'
//! ```

use std::collections::HashMap;

use crate::identity::{AuthError, Identity};

/// A row-level security predicate: filters rows by comparing a user
/// attribute to a table column.
#[derive(Debug, Clone)]
pub struct RowPredicate {
    /// User attribute key (looked up in `Identity.attributes`).
    pub user_attribute: String,
    /// Table column that must match.
    pub column_name: String,
}

/// A row-level security policy for a single table.
#[derive(Debug, Clone)]
pub struct RowSecurityPolicy {
    /// Table this policy applies to.
    pub table_name: String,
    /// Predicates (all must be satisfied — AND semantics).
    pub predicates: Vec<RowPredicate>,
    /// Roles that bypass this policy (e.g., "superuser", "admin").
    pub exempt_roles: Vec<String>,
}

impl RowSecurityPolicy {
    /// Create a new row-level security policy for the given table.
    pub fn new(table_name: impl Into<String>) -> Self {
        Self {
            table_name: table_name.into(),
            predicates: Vec::new(),
            exempt_roles: vec!["superuser".to_string()],
        }
    }

    /// Add a predicate: `column_name = identity.attributes[user_attribute]`.
    pub fn add_predicate(&mut self, user_attribute: &str, column_name: &str) {
        self.predicates.push(RowPredicate {
            user_attribute: user_attribute.to_string(),
            column_name: column_name.to_string(),
        });
    }

    /// Add a role that is exempt from this policy.
    pub fn add_exempt_role(&mut self, role: &str) {
        self.exempt_roles.push(role.to_string());
    }

    /// Check if the identity is exempt from this policy.
    fn is_exempt(&self, identity: &Identity) -> bool {
        self.exempt_roles.iter().any(|r| identity.roles.contains(r))
    }

    /// Generate SQL WHERE clause predicates for the given identity.
    ///
    /// Returns `None` if the identity is exempt. Returns an error if a
    /// required user attribute is missing.
    pub fn generate_where_clause(&self, identity: &Identity) -> Result<Option<String>, AuthError> {
        if self.is_exempt(identity) {
            return Ok(None);
        }

        let mut clauses = Vec::new();
        for pred in &self.predicates {
            let value = identity
                .attributes
                .get(&pred.user_attribute)
                .ok_or_else(|| {
                    AuthError::AccessDenied(format!(
                        "row-level security: user '{}' missing required attribute '{}'",
                        identity.name, pred.user_attribute
                    ))
                })?;
            let column_name = validate_identifier(&pred.column_name)?;
            // Use parameterized-style quoting (single quotes, escape internal quotes)
            let escaped = value.replace('\'', "''");
            clauses.push(format!("{column_name} = '{escaped}'"));
        }

        if clauses.is_empty() {
            Ok(None)
        } else {
            Ok(Some(clauses.join(" AND ")))
        }
    }
}

/// Validate that a string is a safe SQL identifier (letters, digits, underscores).
///
/// Returns the identifier unchanged on success. Used by both row-level and
/// column-level security to prevent SQL injection through column names.
pub fn validate_identifier(identifier: &str) -> Result<&str, AuthError> {
    let mut chars = identifier.chars();
    match chars.next() {
        Some(ch) if ch == '_' || ch.is_ascii_alphabetic() => {}
        _ => {
            return Err(AuthError::AccessDenied(format!(
                "row-level security: invalid SQL identifier '{identifier}'"
            )));
        }
    }

    if !chars.all(|ch| ch == '_' || ch.is_ascii_alphanumeric()) {
        return Err(AuthError::AccessDenied(format!(
            "row-level security: invalid SQL identifier '{identifier}'"
        )));
    }

    Ok(identifier)
}

/// Registry of row-level security policies, keyed by table name.
#[derive(Debug, Clone, Default)]
pub struct RowSecurityRegistry {
    policies: HashMap<String, RowSecurityPolicy>,
}

impl RowSecurityRegistry {
    /// Create an empty registry.
    pub fn new() -> Self {
        Self::default()
    }

    /// Register a policy for a table. Replaces any existing policy for that table.
    pub fn register(&mut self, policy: RowSecurityPolicy) {
        self.policies.insert(policy.table_name.clone(), policy);
    }

    /// Get the WHERE clause for a table and identity.
    ///
    /// Returns `Ok(None)` if no policy exists or the identity is exempt.
    pub fn get_where_clause(
        &self,
        table: &str,
        identity: &Identity,
    ) -> Result<Option<String>, AuthError> {
        match self.policies.get(table) {
            Some(policy) => policy.generate_where_clause(identity),
            None => Ok(None),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::identity::AuthMethod;

    fn tenant_user(tenant: &str) -> Identity {
        Identity::new("alice", AuthMethod::Ldap)
            .with_role("analyst")
            .with_attribute("tenant_id", tenant)
    }

    fn admin_user() -> Identity {
        Identity::new("admin", AuthMethod::Ldap).with_role("superuser")
    }

    #[test]
    fn test_basic_row_security() {
        let mut policy = RowSecurityPolicy::new("orders");
        policy.add_predicate("tenant_id", "tenant_id");

        let user = tenant_user("acme");
        let clause = policy.generate_where_clause(&user).unwrap();
        assert_eq!(clause, Some("tenant_id = 'acme'".to_string()));
    }

    #[test]
    fn test_multiple_predicates() {
        let mut policy = RowSecurityPolicy::new("orders");
        policy.add_predicate("tenant_id", "tenant_id");
        policy.add_predicate("department", "dept");

        let user = tenant_user("acme").with_attribute("department", "engineering");
        let clause = policy.generate_where_clause(&user).unwrap();
        assert_eq!(
            clause,
            Some("tenant_id = 'acme' AND dept = 'engineering'".to_string())
        );
    }

    #[test]
    fn test_superuser_exempt() {
        let mut policy = RowSecurityPolicy::new("orders");
        policy.add_predicate("tenant_id", "tenant_id");

        let admin = admin_user();
        let clause = policy.generate_where_clause(&admin).unwrap();
        assert_eq!(clause, None);
    }

    #[test]
    fn test_missing_attribute_error() {
        let mut policy = RowSecurityPolicy::new("orders");
        policy.add_predicate("tenant_id", "tenant_id");

        // User without tenant_id attribute
        let user = Identity::new("nobody", AuthMethod::Anonymous);
        let result = policy.generate_where_clause(&user);
        assert!(result.is_err());
    }

    #[test]
    fn test_sql_injection_prevention() {
        let mut policy = RowSecurityPolicy::new("orders");
        policy.add_predicate("tenant_id", "tenant_id");

        // Attempt SQL injection via attribute value
        let user = tenant_user("'; DROP TABLE orders; --");
        let clause = policy.generate_where_clause(&user).unwrap().unwrap();
        // Single quotes should be escaped
        assert_eq!(clause, "tenant_id = '''; DROP TABLE orders; --'");
    }

    #[test]
    fn test_registry() {
        let mut registry = RowSecurityRegistry::new();

        let mut policy = RowSecurityPolicy::new("orders");
        policy.add_predicate("tenant_id", "tenant_id");
        registry.register(policy);

        let user = tenant_user("acme");
        assert!(registry
            .get_where_clause("orders", &user)
            .unwrap()
            .is_some());
        assert!(registry
            .get_where_clause("other_table", &user)
            .unwrap()
            .is_none());
    }

    #[test]
    fn test_invalid_column_identifier_is_rejected() {
        let mut policy = RowSecurityPolicy::new("orders");
        policy.add_predicate("tenant_id", "tenant_id OR 1=1");

        let user = tenant_user("acme");
        let err = policy.generate_where_clause(&user).unwrap_err();
        assert!(err.to_string().contains("invalid SQL identifier"));
    }
}
