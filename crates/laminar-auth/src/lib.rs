//! # LaminarDB Authentication & Authorization

#![deny(missing_docs)]
#![warn(clippy::all, clippy::pedantic)]

/// Authentication mechanisms
pub mod authn {
    //! JWT, mTLS, and other auth methods
}

/// Authorization (RBAC/ABAC)
pub mod authz {
    //! Role and attribute based access control
}

/// Row-level security
pub mod rls {
    //! Fine-grained access control
}