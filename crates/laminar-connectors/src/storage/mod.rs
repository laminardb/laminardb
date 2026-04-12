//! Cloud storage infrastructure for lakehouse connectors.

pub mod masking;
pub mod provider;
pub mod resolver;
pub mod validation;

// Re-export primary types at module level.
pub use masking::SecretMasker;
pub use provider::StorageProvider;
pub use resolver::{ResolvedStorageOptions, StorageCredentialResolver};
pub use validation::{
    CloudConfigValidator, CloudValidationError, CloudValidationResult, CloudValidationWarning,
};
