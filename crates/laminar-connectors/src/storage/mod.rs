//! Cloud storage infrastructure for lakehouse connectors.

pub mod masking;
pub mod provider;
pub mod resolver;
pub mod validation;

// Re-export primary types at module level.
pub use laminar_core::storage_location::{
    AdaptedStorageLocation, EndpointOverride, StorageConsumer, StorageEndpointClass,
    StorageLocation, StorageLocationError,
};
pub use masking::SecretMasker;
pub use provider::StorageProvider;
pub use resolver::{AuthSource, ResolvedStorageOptions, StorageCredentialResolver};
pub use validation::{
    CloudConfigValidator, CloudValidationError, CloudValidationResult, CloudValidationWarning,
};
