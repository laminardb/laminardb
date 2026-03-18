//! Column-Level Security.
//!
//! Provides per-table column masking and redaction based on the user's
//! roles. When a non-exempt user queries a protected table, column
//! transforms are applied to mask sensitive data.
//!
//! # Example
//!
//! For role `analyst` on table `users`, column `email` is masked:
//! ```text
//! "alice@example.com" → "***@example.com"
//! ```

use std::collections::HashMap;

use crate::identity::Identity;

/// How a column should be masked.
#[derive(Debug, Clone)]
pub enum MaskingStrategy {
    /// Replace the entire value with a fixed string (e.g., "***").
    Redact(String),
    /// Keep only the domain part of an email: `user@domain` → `***@domain`.
    EmailDomain,
    /// Replace with NULL.
    Nullify,
    /// Truncate to first N characters, pad with `*`.
    Truncate(usize),
    /// Custom SQL expression to apply (e.g., `CONCAT(LEFT(col, 2), '****')`).
    Expression(String),
}

/// A column masking rule for a specific column in a table.
#[derive(Debug, Clone)]
pub struct ColumnMask {
    /// Column name to mask.
    pub column_name: String,
    /// Masking strategy to apply.
    pub strategy: MaskingStrategy,
}

/// Column-level security policy for a single table.
#[derive(Debug, Clone)]
pub struct ColumnSecurityPolicy {
    /// Table this policy applies to.
    pub table_name: String,
    /// Per-role masking rules: role → list of column masks.
    /// If a role is not listed, no masking is applied for that role.
    role_masks: HashMap<String, Vec<ColumnMask>>,
    /// Roles that bypass all column masking.
    pub exempt_roles: Vec<String>,
}

impl ColumnSecurityPolicy {
    /// Create a new column-level security policy for the given table.
    pub fn new(table_name: impl Into<String>) -> Self {
        Self {
            table_name: table_name.into(),
            role_masks: HashMap::new(),
            exempt_roles: vec!["superuser".to_string()],
        }
    }

    /// Add a column mask for a specific role.
    pub fn add_mask(&mut self, role: &str, mask: ColumnMask) {
        self.role_masks
            .entry(role.to_string())
            .or_default()
            .push(mask);
    }

    /// Add a role that is exempt from column masking.
    pub fn add_exempt_role(&mut self, role: &str) {
        self.exempt_roles.push(role.to_string());
    }

    /// Check if the identity is exempt from this policy.
    fn is_exempt(&self, identity: &Identity) -> bool {
        self.exempt_roles.iter().any(|r| identity.roles.contains(r))
    }

    /// Get the effective column masks for a given identity.
    ///
    /// Returns the union of masks for all of the identity's roles.
    /// Returns an empty map if exempt.
    pub fn effective_masks(&self, identity: &Identity) -> HashMap<String, &ColumnMask> {
        if self.is_exempt(identity) {
            return HashMap::new();
        }

        let mut masks: HashMap<String, &ColumnMask> = HashMap::new();
        let mut roles: Vec<&str> = identity.roles.iter().map(String::as_str).collect();
        roles.sort_unstable();
        for role in roles {
            if let Some(role_masks) = self.role_masks.get(role) {
                for mask in role_masks {
                    // First mask wins for a given column
                    masks.entry(mask.column_name.clone()).or_insert(mask);
                }
            }
        }
        masks
    }

    /// Generate a SQL projection expression for a column, applying masking if needed.
    ///
    /// Returns `None` if no masking is needed (column passes through unchanged).
    pub fn masked_projection(&self, identity: &Identity, column: &str) -> Option<String> {
        let masks = self.effective_masks(identity);
        let mask = masks.get(column)?;

        Some(match &mask.strategy {
            MaskingStrategy::Redact(replacement) => {
                format!("'{}' AS {}", replacement.replace('\'', "''"), column)
            }
            MaskingStrategy::EmailDomain => {
                format!(
                    "CONCAT('***@', SUBSTRING({col} FROM POSITION('@' IN {col}) + 1)) AS {col}",
                    col = column
                )
            }
            MaskingStrategy::Nullify => format!("NULL AS {column}"),
            MaskingStrategy::Truncate(n) => {
                format!("CONCAT(LEFT({col}, {n}), '****') AS {col}", col = column)
            }
            MaskingStrategy::Expression(expr) => format!("({expr}) AS {column}"),
        })
    }
}

/// Registry of column-level security policies, keyed by table name.
#[derive(Debug, Clone, Default)]
pub struct ColumnSecurityRegistry {
    policies: HashMap<String, ColumnSecurityPolicy>,
}

impl ColumnSecurityRegistry {
    /// Create an empty registry.
    pub fn new() -> Self {
        Self::default()
    }

    /// Register a policy for a table. Replaces any existing policy.
    pub fn register(&mut self, policy: ColumnSecurityPolicy) {
        self.policies.insert(policy.table_name.clone(), policy);
    }

    /// Get the masked projection for a column on a table.
    ///
    /// Returns `None` if no masking is needed.
    pub fn masked_projection(
        &self,
        table: &str,
        identity: &Identity,
        column: &str,
    ) -> Option<String> {
        self.policies
            .get(table)
            .and_then(|p| p.masked_projection(identity, column))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::identity::AuthMethod;

    fn analyst() -> Identity {
        Identity::new("alice", AuthMethod::Ldap).with_role("analyst")
    }

    fn admin() -> Identity {
        Identity::new("admin", AuthMethod::Ldap).with_role("superuser")
    }

    #[test]
    fn test_redact_mask() {
        let mut policy = ColumnSecurityPolicy::new("users");
        policy.add_mask(
            "analyst",
            ColumnMask {
                column_name: "ssn".to_string(),
                strategy: MaskingStrategy::Redact("***-**-****".to_string()),
            },
        );

        let proj = policy.masked_projection(&analyst(), "ssn");
        assert_eq!(proj, Some("'***-**-****' AS ssn".to_string()));
    }

    #[test]
    fn test_email_domain_mask() {
        let mut policy = ColumnSecurityPolicy::new("users");
        policy.add_mask(
            "analyst",
            ColumnMask {
                column_name: "email".to_string(),
                strategy: MaskingStrategy::EmailDomain,
            },
        );

        let proj = policy.masked_projection(&analyst(), "email");
        assert!(proj.unwrap().contains("***@"));
    }

    #[test]
    fn test_nullify_mask() {
        let mut policy = ColumnSecurityPolicy::new("users");
        policy.add_mask(
            "analyst",
            ColumnMask {
                column_name: "phone".to_string(),
                strategy: MaskingStrategy::Nullify,
            },
        );

        let proj = policy.masked_projection(&analyst(), "phone");
        assert_eq!(proj, Some("NULL AS phone".to_string()));
    }

    #[test]
    fn test_truncate_mask() {
        let mut policy = ColumnSecurityPolicy::new("users");
        policy.add_mask(
            "analyst",
            ColumnMask {
                column_name: "name".to_string(),
                strategy: MaskingStrategy::Truncate(2),
            },
        );

        let proj = policy.masked_projection(&analyst(), "name");
        assert_eq!(
            proj,
            Some("CONCAT(LEFT(name, 2), '****') AS name".to_string())
        );
    }

    #[test]
    fn test_superuser_exempt() {
        let mut policy = ColumnSecurityPolicy::new("users");
        policy.add_mask(
            "analyst",
            ColumnMask {
                column_name: "ssn".to_string(),
                strategy: MaskingStrategy::Redact("***".to_string()),
            },
        );

        let proj = policy.masked_projection(&admin(), "ssn");
        assert_eq!(proj, None);
    }

    #[test]
    fn test_no_mask_for_unmasked_column() {
        let mut policy = ColumnSecurityPolicy::new("users");
        policy.add_mask(
            "analyst",
            ColumnMask {
                column_name: "ssn".to_string(),
                strategy: MaskingStrategy::Redact("***".to_string()),
            },
        );

        // "name" column has no mask
        let proj = policy.masked_projection(&analyst(), "name");
        assert_eq!(proj, None);
    }

    #[test]
    fn test_registry() {
        let mut registry = ColumnSecurityRegistry::new();
        let mut policy = ColumnSecurityPolicy::new("users");
        policy.add_mask(
            "analyst",
            ColumnMask {
                column_name: "email".to_string(),
                strategy: MaskingStrategy::EmailDomain,
            },
        );
        registry.register(policy);

        assert!(registry
            .masked_projection("users", &analyst(), "email")
            .is_some());
        assert!(registry
            .masked_projection("users", &analyst(), "name")
            .is_none());
        assert!(registry
            .masked_projection("other", &analyst(), "email")
            .is_none());
    }

    #[test]
    fn test_effective_masks_use_deterministic_role_order() {
        let mut policy = ColumnSecurityPolicy::new("users");
        policy.add_mask(
            "alpha",
            ColumnMask {
                column_name: "ssn".to_string(),
                strategy: MaskingStrategy::Redact("ALPHA".to_string()),
            },
        );
        policy.add_mask(
            "beta",
            ColumnMask {
                column_name: "ssn".to_string(),
                strategy: MaskingStrategy::Redact("BETA".to_string()),
            },
        );

        let first = Identity::new("alice", AuthMethod::Ldap)
            .with_role("alpha")
            .with_role("beta");
        let second = Identity::new("alice", AuthMethod::Ldap)
            .with_role("beta")
            .with_role("alpha");

        assert_eq!(
            policy.masked_projection(&first, "ssn"),
            Some("'ALPHA' AS ssn".to_string())
        );
        assert_eq!(
            policy.masked_projection(&second, "ssn"),
            Some("'ALPHA' AS ssn".to_string())
        );
    }
}
