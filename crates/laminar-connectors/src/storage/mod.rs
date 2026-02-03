//! Cloud storage infrastructure for lakehouse connectors.
//!
//! Provides shared credential resolution, validation, and secret masking
//! for all connectors that interact with cloud object stores (S3, Azure ADLS,
//! GCS, MinIO, local filesystem).
//!
//! # Architecture
//!
//! ```text
//! SQL WITH clause          Environment vars       Instance metadata
//! (storage.* keys)         (AWS_*, AZURE_*, etc)  (IAM role, MI, WI)
//!       |                        |                       |
//!       v                        v                       v
//!  StorageCredentialResolver (priority: explicit > env > instance)
//!       |
//!       v
//!  CloudConfigValidator (per-provider required fields)
//!       |
//!       v
//!  SecretMasker (redacted Debug/Display for safe logging)
//! ```
//!
//! # Usage
//!
//! ```rust
//! use laminar_connectors::storage::{
//!     StorageProvider, StorageCredentialResolver, CloudConfigValidator, SecretMasker,
//! };
//! use std::collections::HashMap;
//!
//! // 1. Resolve credentials (explicit config + env var fallback).
//! let explicit_opts = HashMap::new(); // from SQL WITH clause
//! let resolved = StorageCredentialResolver::resolve("s3://my-bucket/path", &explicit_opts);
//!
//! // 2. Validate for the detected provider.
//! let validation = CloudConfigValidator::validate(&resolved);
//! if !validation.is_valid() {
//!     eprintln!("Config errors: {}", validation.error_message());
//! }
//!
//! // 3. Safe logging with secrets redacted.
//! println!("Options: {}", SecretMasker::display_map(&resolved.options));
//! ```

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
