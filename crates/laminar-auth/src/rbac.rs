//! Role-Based Access Control (RBAC).
//!
//! Defines roles and permissions, then checks whether an [`Identity`]
//! has the required permission for an operation on a resource.

use std::collections::{HashMap, HashSet};

use crate::identity::{AuthError, Identity};

/// A named permission (e.g., "SELECT", "INSERT", "CREATE_SOURCE").
pub type Permission = String;

/// RBAC policy: maps roles to sets of permissions on resources.
#[derive(Debug, Clone, Default)]
pub struct RbacPolicy {
    /// Global role → permissions (apply to all resources).
    global_grants: HashMap<String, HashSet<Permission>>,

    /// Resource-specific grants: resource_name → role → permissions.
    resource_grants: HashMap<String, HashMap<String, HashSet<Permission>>>,
}

impl RbacPolicy {
    /// Create an empty RBAC policy.
    pub fn new() -> Self {
        Self::default()
    }

    /// Grant global permissions to a role.
    pub fn grant_global(&mut self, role: &str, permissions: &[&str]) {
        let entry = self.global_grants.entry(role.to_string()).or_default();
        for p in permissions {
            entry.insert((*p).to_string());
        }
    }

    /// Grant resource-specific permissions to a role.
    pub fn grant_resource(&mut self, resource: &str, role: &str, permissions: &[&str]) {
        let resource_entry = self
            .resource_grants
            .entry(resource.to_string())
            .or_default();
        let role_entry = resource_entry.entry(role.to_string()).or_default();
        for p in permissions {
            role_entry.insert((*p).to_string());
        }
    }

    /// Check whether the identity has a specific permission, optionally scoped to a resource.
    ///
    /// Superusers bypass all checks.
    pub fn check(
        &self,
        identity: &Identity,
        permission: &str,
        resource: Option<&str>,
    ) -> Result<(), AuthError> {
        // Superuser bypasses all RBAC checks
        if identity.is_superuser() {
            return Ok(());
        }

        // Check global grants
        for role in &identity.roles {
            if let Some(perms) = self.global_grants.get(role) {
                if perms.contains(permission) || perms.contains("*") {
                    return Ok(());
                }
            }
        }

        // Check resource-specific grants
        if let Some(resource_name) = resource {
            if let Some(resource_grants) = self.resource_grants.get(resource_name) {
                for role in &identity.roles {
                    if let Some(perms) = resource_grants.get(role) {
                        if perms.contains(permission) || perms.contains("*") {
                            return Ok(());
                        }
                    }
                }
            }
        }

        Err(AuthError::AccessDenied(format!(
            "user '{}' lacks permission '{}' on {}",
            identity.name,
            permission,
            resource.unwrap_or("(global)")
        )))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::identity::AuthMethod;

    fn analyst_identity() -> Identity {
        Identity::new("alice", AuthMethod::Ldap)
            .with_role("analyst")
            .with_role("viewer")
    }

    fn admin_identity() -> Identity {
        Identity::new("admin", AuthMethod::Ldap).with_role("admin")
    }

    fn superuser_identity() -> Identity {
        Identity::new("root", AuthMethod::Anonymous).with_role("superuser")
    }

    #[test]
    fn test_global_grant() {
        let mut policy = RbacPolicy::new();
        policy.grant_global("analyst", &["SELECT", "DESCRIBE"]);

        let id = analyst_identity();
        assert!(policy.check(&id, "SELECT", None).is_ok());
        assert!(policy.check(&id, "DESCRIBE", None).is_ok());
        assert!(policy.check(&id, "INSERT", None).is_err());
    }

    #[test]
    fn test_resource_grant() {
        let mut policy = RbacPolicy::new();
        policy.grant_resource("trades", "analyst", &["SELECT"]);

        let id = analyst_identity();
        assert!(policy.check(&id, "SELECT", Some("trades")).is_ok());
        assert!(policy.check(&id, "SELECT", Some("orders")).is_err());
        assert!(policy.check(&id, "INSERT", Some("trades")).is_err());
    }

    #[test]
    fn test_wildcard_permission() {
        let mut policy = RbacPolicy::new();
        policy.grant_global("admin", &["*"]);

        let id = admin_identity();
        assert!(policy.check(&id, "SELECT", None).is_ok());
        assert!(policy.check(&id, "DROP", None).is_ok());
    }

    #[test]
    fn test_superuser_bypass() {
        let policy = RbacPolicy::new(); // empty policy
        let id = superuser_identity();
        // Superuser passes even with no grants
        assert!(policy.check(&id, "DROP", Some("critical_table")).is_ok());
    }

    #[test]
    fn test_no_role_denied() {
        let policy = RbacPolicy::new();
        let id = Identity::new("nobody", AuthMethod::Anonymous);
        assert!(policy.check(&id, "SELECT", None).is_err());
    }
}
