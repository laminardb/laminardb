//! Role-based access control (RBAC).
//!
//! Maps [`Role`]s to sets of [`Permission`]s, then checks whether an
//! [`Identity`] is authorized to perform a given action.

use std::collections::{HashMap, HashSet};

use serde::{Deserialize, Serialize};

use crate::identity::Identity;

/// A role assignable to users.
///
/// Built-in roles have well-known semantics. Custom roles allow
/// fine-grained organization-specific policies.
#[derive(Debug, Clone, Eq, PartialEq, Hash, Serialize, Deserialize)]
pub enum Role {
    /// Full administrative access.
    Admin,
    /// Read and write access to data and DDL.
    ReadWrite,
    /// Read-only access (queries only).
    ReadOnly,
    /// A custom role defined by the operator.
    Custom(String),
}

impl Role {
    /// Parse a role from a string. Recognized names are case-insensitive:
    /// `admin`, `read_write` / `readwrite`, `read_only` / `readonly`.
    /// Anything else becomes `Custom(s)`.
    pub fn parse(s: &str) -> Self {
        match s.to_lowercase().as_str() {
            "admin" => Self::Admin,
            "read_write" | "readwrite" => Self::ReadWrite,
            "read_only" | "readonly" => Self::ReadOnly,
            _ => Self::Custom(s.to_string()),
        }
    }
}

impl std::fmt::Display for Role {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Admin => write!(f, "admin"),
            Self::ReadWrite => write!(f, "read_write"),
            Self::ReadOnly => write!(f, "read_only"),
            Self::Custom(name) => write!(f, "{name}"),
        }
    }
}

/// Permissions that can be granted to roles.
#[derive(Debug, Clone, Eq, PartialEq, Hash, Serialize, Deserialize)]
pub enum Permission {
    /// Create a new source.
    CreateSource,
    /// Create a new stream/pipeline.
    CreateStream,
    /// Create a new sink.
    CreateSink,
    /// Execute SQL queries (SELECT).
    ExecuteQuery,
    /// Execute DDL statements (CREATE, DROP, ALTER).
    ExecuteDdl,
    /// Trigger checkpoints.
    TriggerCheckpoint,
    /// Reload server configuration.
    ReloadConfig,
    /// View server metrics and status.
    ViewMetrics,
    /// Manage users and roles.
    ManageUsers,
    /// Manage cluster (delta mode).
    ManageCluster,
}

/// RBAC policy: maps roles to sets of permissions.
///
/// Use [`RbacPolicy::default_policy`] for sensible defaults, or build
/// a custom policy with [`RbacPolicy::new`] and [`RbacPolicy::grant`].
#[derive(Debug, Clone)]
pub struct RbacPolicy {
    grants: HashMap<Role, HashSet<Permission>>,
}

impl RbacPolicy {
    /// Create an empty policy (no roles have any permissions).
    pub fn new() -> Self {
        Self {
            grants: HashMap::new(),
        }
    }

    /// Create the default policy with sensible role-permission mappings:
    ///
    /// - **Admin**: all permissions
    /// - **`ReadWrite`**: create sources/streams/sinks, execute queries/DDL, trigger checkpoints
    /// - **`ReadOnly`**: execute queries, view metrics
    pub fn default_policy() -> Self {
        let mut policy = Self::new();

        // Admin: everything
        policy.grant(Role::Admin, Permission::CreateSource);
        policy.grant(Role::Admin, Permission::CreateStream);
        policy.grant(Role::Admin, Permission::CreateSink);
        policy.grant(Role::Admin, Permission::ExecuteQuery);
        policy.grant(Role::Admin, Permission::ExecuteDdl);
        policy.grant(Role::Admin, Permission::TriggerCheckpoint);
        policy.grant(Role::Admin, Permission::ReloadConfig);
        policy.grant(Role::Admin, Permission::ViewMetrics);
        policy.grant(Role::Admin, Permission::ManageUsers);
        policy.grant(Role::Admin, Permission::ManageCluster);

        // ReadWrite: data operations
        policy.grant(Role::ReadWrite, Permission::CreateSource);
        policy.grant(Role::ReadWrite, Permission::CreateStream);
        policy.grant(Role::ReadWrite, Permission::CreateSink);
        policy.grant(Role::ReadWrite, Permission::ExecuteQuery);
        policy.grant(Role::ReadWrite, Permission::ExecuteDdl);
        policy.grant(Role::ReadWrite, Permission::TriggerCheckpoint);
        policy.grant(Role::ReadWrite, Permission::ViewMetrics);

        // ReadOnly: queries and metrics
        policy.grant(Role::ReadOnly, Permission::ExecuteQuery);
        policy.grant(Role::ReadOnly, Permission::ViewMetrics);

        policy
    }

    /// Grant a permission to a role.
    pub fn grant(&mut self, role: Role, permission: Permission) {
        self.grants.entry(role).or_default().insert(permission);
    }

    /// Revoke a permission from a role.
    pub fn revoke(&mut self, role: &Role, permission: &Permission) {
        if let Some(perms) = self.grants.get_mut(role) {
            perms.remove(permission);
        }
    }

    /// Check whether a given identity has the specified permission.
    ///
    /// Returns `true` if **any** of the identity's roles grants the
    /// requested permission.
    pub fn is_permitted(&self, identity: &Identity, permission: &Permission) -> bool {
        for role in &identity.roles {
            if let Some(perms) = self.grants.get(role) {
                if perms.contains(permission) {
                    return true;
                }
            }
        }
        false
    }

    /// Get all permissions granted to a specific role.
    pub fn permissions_for(&self, role: &Role) -> HashSet<Permission> {
        self.grants.get(role).cloned().unwrap_or_default()
    }
}

impl Default for RbacPolicy {
    fn default() -> Self {
        Self::default_policy()
    }
}

/// Error returned when an authorization check fails.
#[derive(Debug, thiserror::Error)]
#[error("access denied: {user_id} lacks permission {permission:?} (roles: {roles})")]
pub struct AccessDenied {
    /// The user who was denied.
    pub user_id: String,
    /// The permission that was required.
    pub permission: Permission,
    /// Comma-separated list of the user's roles.
    pub roles: String,
}

/// Check authorization and produce a structured error on failure.
///
/// This is a convenience function that combines [`RbacPolicy::is_permitted`]
/// with error construction.
pub fn check_permission(
    policy: &RbacPolicy,
    identity: &Identity,
    permission: Permission,
) -> Result<(), AccessDenied> {
    if policy.is_permitted(identity, &permission) {
        Ok(())
    } else {
        Err(AccessDenied {
            user_id: identity.user_id.clone(),
            permission,
            roles: identity
                .roles
                .iter()
                .map(ToString::to_string)
                .collect::<Vec<_>>()
                .join(", "),
        })
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_policy_admin_has_all() {
        let policy = RbacPolicy::default_policy();
        let admin = Identity::new("admin").with_role(Role::Admin);

        assert!(policy.is_permitted(&admin, &Permission::CreateSource));
        assert!(policy.is_permitted(&admin, &Permission::ManageUsers));
        assert!(policy.is_permitted(&admin, &Permission::ExecuteQuery));
        assert!(policy.is_permitted(&admin, &Permission::ManageCluster));
    }

    #[test]
    fn test_default_policy_readonly() {
        let policy = RbacPolicy::default_policy();
        let reader = Identity::new("reader").with_role(Role::ReadOnly);

        assert!(policy.is_permitted(&reader, &Permission::ExecuteQuery));
        assert!(policy.is_permitted(&reader, &Permission::ViewMetrics));
        assert!(!policy.is_permitted(&reader, &Permission::CreateSource));
        assert!(!policy.is_permitted(&reader, &Permission::ManageUsers));
        assert!(!policy.is_permitted(&reader, &Permission::ExecuteDdl));
    }

    #[test]
    fn test_default_policy_readwrite() {
        let policy = RbacPolicy::default_policy();
        let rw = Identity::new("dev").with_role(Role::ReadWrite);

        assert!(policy.is_permitted(&rw, &Permission::CreateSource));
        assert!(policy.is_permitted(&rw, &Permission::ExecuteQuery));
        assert!(policy.is_permitted(&rw, &Permission::TriggerCheckpoint));
        assert!(!policy.is_permitted(&rw, &Permission::ManageUsers));
        assert!(!policy.is_permitted(&rw, &Permission::ReloadConfig));
    }

    #[test]
    fn test_multi_role_union() {
        let policy = RbacPolicy::default_policy();
        let user = Identity::new("multi")
            .with_role(Role::ReadOnly)
            .with_role(Role::Custom("ops".to_string()));

        // ReadOnly grants ExecuteQuery
        assert!(policy.is_permitted(&user, &Permission::ExecuteQuery));
        // Custom role has no grants by default
        assert!(!policy.is_permitted(&user, &Permission::CreateSource));
    }

    #[test]
    fn test_custom_role_grant() {
        let mut policy = RbacPolicy::new();
        let ops = Role::Custom("ops".to_string());
        policy.grant(ops.clone(), Permission::TriggerCheckpoint);
        policy.grant(ops.clone(), Permission::ReloadConfig);
        policy.grant(ops, Permission::ViewMetrics);

        let user = Identity::new("oncall").with_role(Role::Custom("ops".to_string()));
        assert!(policy.is_permitted(&user, &Permission::TriggerCheckpoint));
        assert!(policy.is_permitted(&user, &Permission::ReloadConfig));
        assert!(!policy.is_permitted(&user, &Permission::ExecuteQuery));
    }

    #[test]
    fn test_revoke() {
        let mut policy = RbacPolicy::default_policy();
        policy.revoke(&Role::ReadWrite, &Permission::CreateSource);

        let rw = Identity::new("dev").with_role(Role::ReadWrite);
        assert!(!policy.is_permitted(&rw, &Permission::CreateSource));
        assert!(policy.is_permitted(&rw, &Permission::ExecuteQuery));
    }

    #[test]
    fn test_check_permission_ok() {
        let policy = RbacPolicy::default_policy();
        let admin = Identity::new("admin").with_role(Role::Admin);
        assert!(check_permission(&policy, &admin, Permission::ManageUsers).is_ok());
    }

    #[test]
    fn test_check_permission_denied() {
        let policy = RbacPolicy::default_policy();
        let reader = Identity::new("reader").with_role(Role::ReadOnly);
        let err = check_permission(&policy, &reader, Permission::CreateSource).unwrap_err();
        assert_eq!(err.user_id, "reader");
        assert!(err.to_string().contains("access denied"));
    }

    #[test]
    fn test_role_from_str() {
        assert_eq!(Role::parse("admin"), Role::Admin);
        assert_eq!(Role::parse("ADMIN"), Role::Admin);
        assert_eq!(Role::parse("read_write"), Role::ReadWrite);
        assert_eq!(Role::parse("readwrite"), Role::ReadWrite);
        assert_eq!(Role::parse("read_only"), Role::ReadOnly);
        assert_eq!(Role::parse("readonly"), Role::ReadOnly);
        assert_eq!(
            Role::parse("custom_role"),
            Role::Custom("custom_role".to_string())
        );
    }

    #[test]
    fn test_role_display() {
        assert_eq!(Role::Admin.to_string(), "admin");
        assert_eq!(Role::ReadWrite.to_string(), "read_write");
        assert_eq!(Role::ReadOnly.to_string(), "read_only");
        assert_eq!(Role::Custom("ops".to_string()).to_string(), "ops");
    }

    #[test]
    fn test_no_roles_no_permissions() {
        let policy = RbacPolicy::default_policy();
        let nobody = Identity::new("nobody");
        assert!(!policy.is_permitted(&nobody, &Permission::ExecuteQuery));
    }
}
