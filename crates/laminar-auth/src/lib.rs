//! Authentication, authorization, and key management for LaminarDB.
//!
//! This crate provides:
//!
//! - **Authentication providers**: mTLS certificate extraction, LDAP bind+search
//! - **Authorization**: Role-based (RBAC) and attribute-based (ABAC) access control
//! - **Row/Column-level security**: Per-table predicates and column masking
//! - **Key management**: Pluggable key storage for encryption at rest
//!
//! # Feature flags
//!
//! | Flag | Purpose |
//! |------|---------|
//! | `mtls` | mTLS authentication via rustls + x509 |
//! | `ldap` | LDAP authentication via ldap3 |
//! | `security` | Enables all security features |

#![deny(missing_docs)]
#![allow(clippy::disallowed_types)] // cold path: auth/authz configuration and evaluation only

pub mod abac;
pub mod column_security;
pub mod identity;
pub mod key_management;
#[cfg(feature = "ldap")]
pub mod ldap_auth;
#[cfg(feature = "mtls")]
pub mod mtls;
pub mod rbac;
pub mod row_security;

pub use identity::{AuthError, AuthProvider, Identity};
