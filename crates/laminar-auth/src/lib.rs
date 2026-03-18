//! # `LaminarDB` Authentication & Authorization
//!
//! Security layer for `LaminarDB` providing:
//!
//! - **Authentication** ([`AuthProvider`] trait, [`Identity`]) -- pluggable authentication backends
//! - **JWT authentication** ([`jwt`]) -- validate JWT tokens, extract claims
//! - **RBAC** ([`rbac`]) -- role-based access control with permission checks
//! - **Audit logging** ([`audit`]) -- structured audit trail for security events
//! - **Encryption at rest** ([`encryption`]) -- AES-256-GCM for checkpoint/WAL data
//!
//! All features are gated behind the `auth` feature flag.
//!
//! # Quick Start
//!
//! ```toml
//! [dependencies]
//! laminar-auth = { path = "../laminar-auth", features = ["auth"] }
//! ```

#![deny(missing_docs)]
#![warn(clippy::all, clippy::pedantic)]
#![allow(clippy::disallowed_types)] // cold path: auth/RBAC/audit code
#![allow(
    clippy::module_name_repetitions,
    clippy::must_use_candidate,
    clippy::missing_errors_doc,
    clippy::missing_panics_doc
)]

/// Core authentication types and traits.
pub mod identity;

/// JWT-based authentication provider.
#[cfg(feature = "auth")]
pub mod jwt;

/// Role-based access control (RBAC).
pub mod rbac;

/// Structured audit logging.
pub mod audit;

/// Encryption at rest (AES-256-GCM).
#[cfg(feature = "auth")]
pub mod encryption;

// Re-exports
pub use identity::{AuthError, AuthProvider, Credentials, Identity};
pub use rbac::{Permission, RbacPolicy, Role};

#[cfg(feature = "auth")]
pub use jwt::{JwtAuthProvider, JwtConfig};

#[cfg(feature = "auth")]
pub use encryption::{DecryptionError, EncryptionError, EncryptionLayer};
