//! # `LaminarDB` Authentication & Authorization

#![deny(missing_docs)]
#![warn(clippy::all, clippy::pedantic)]
#![allow(clippy::disallowed_types)]

/// Authentication mechanisms - JWT, mTLS, and other auth methods
pub mod authn;

/// Authorization (RBAC/ABAC) - Role and attribute based access control
pub mod authz;

/// Row-level security - Fine-grained access control
pub mod rls;
