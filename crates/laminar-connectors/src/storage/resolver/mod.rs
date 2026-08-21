//! Cloud storage credential resolver.
//!
//! [`StorageCredentialResolver`] merges explicit `storage.*` config options with
//! environment variable fallbacks, producing a [`ResolvedStorageOptions`] ready
//! for consumption by `object_store` / `deltalake`.
//!
//! Resolution priority chain:
//! 1. Explicit connector options (`storage.*` keys)
//! 2. Environment variables (`AWS_ACCESS_KEY_ID`, etc.)
//! 3. Instance metadata / default credential providers (handled by `object_store`)
#![allow(clippy::disallowed_types)] // cold path: storage configuration

use std::collections::HashMap;

use super::provider::StorageProvider;

/// AWS S3 environment variable fallbacks.
///
/// Maps option key (as used by `object_store`/`deltalake`) to env var name.
const AWS_ENV_MAPPING: &[(&str, &str)] = &[
    ("aws_access_key_id", "AWS_ACCESS_KEY_ID"),
    ("aws_secret_access_key", "AWS_SECRET_ACCESS_KEY"),
    ("aws_region", "AWS_REGION"),
    ("aws_session_token", "AWS_SESSION_TOKEN"),
    ("aws_endpoint", "AWS_ENDPOINT_URL"),
    ("aws_profile", "AWS_PROFILE"),
    ("aws_s3_allow_unsafe_rename", "AWS_S3_ALLOW_UNSAFE_RENAME"),
];

/// Azure ADLS environment variable fallbacks.
const AZURE_ENV_MAPPING: &[(&str, &str)] = &[
    ("azure_storage_account_name", "AZURE_STORAGE_ACCOUNT_NAME"),
    ("azure_storage_account_key", "AZURE_STORAGE_ACCOUNT_KEY"),
    ("azure_storage_sas_token", "AZURE_STORAGE_SAS_TOKEN"),
    ("azure_storage_client_id", "AZURE_CLIENT_ID"),
    ("azure_storage_tenant_id", "AZURE_TENANT_ID"),
    ("azure_storage_client_secret", "AZURE_CLIENT_SECRET"),
];

/// GCS environment variable fallbacks.
const GCS_ENV_MAPPING: &[(&str, &str)] = &[
    (
        "google_service_account_path",
        "GOOGLE_APPLICATION_CREDENTIALS",
    ),
    ("google_service_account_key", "GOOGLE_SERVICE_ACCOUNT_KEY"),
];

/// Resolved storage credentials ready for `object_store` / `deltalake`.
#[derive(Debug, Clone)]
pub struct ResolvedStorageOptions {
    /// Detected cloud provider.
    pub provider: StorageProvider,
    /// Merged options (explicit config + env vars).
    /// Keys match what `deltalake`/`object_store` expect.
    pub options: HashMap<String, String>,
    /// Keys that were resolved from environment variables (not explicit config).
    pub env_resolved_keys: Vec<String>,
}

impl ResolvedStorageOptions {
    /// Returns true if any credentials were found (explicit or from env).
    #[must_use]
    pub fn has_credentials(&self) -> bool {
        match self.provider {
            StorageProvider::AwsS3 => {
                self.options.contains_key("aws_access_key_id")
                    || self.options.contains_key("aws_profile")
            }
            StorageProvider::AzureAdls => {
                self.options.contains_key("azure_storage_account_key")
                    || self.options.contains_key("azure_storage_sas_token")
                    || self.options.contains_key("azure_storage_client_id")
            }
            StorageProvider::Gcs => {
                self.options.contains_key("google_service_account_path")
                    || self.options.contains_key("google_service_account_key")
            }
            StorageProvider::Local => false,
        }
    }
}

/// Storage credential resolver.
///
/// Resolves credentials by priority chain:
/// 1. Explicit `storage.*` connector options
/// 2. Environment variables
/// 3. Instance metadata / default credential provider (handled downstream by `object_store`)
pub struct StorageCredentialResolver;

impl StorageCredentialResolver {
    /// Resolves storage credentials for the given table path.
    ///
    /// Merges explicit options with environment variable fallbacks
    /// appropriate for the detected cloud provider.
    ///
    /// # Arguments
    ///
    /// * `table_path` - URI of the table (`s3://`, `az://`, `gs://`, or local path)
    /// * `explicit_options` - Connector options (`storage.` prefix already stripped)
    ///
    /// # Returns
    ///
    /// [`ResolvedStorageOptions`] with merged credentials.
    #[must_use]
    pub fn resolve(
        table_path: &str,
        explicit_options: &HashMap<String, String>,
    ) -> ResolvedStorageOptions {
        let provider = StorageProvider::detect(table_path);

        if provider == StorageProvider::Local {
            return ResolvedStorageOptions {
                provider,
                options: explicit_options.clone(),
                env_resolved_keys: Vec::new(),
            };
        }

        let env_mapping = match provider {
            StorageProvider::AwsS3 => AWS_ENV_MAPPING,
            StorageProvider::AzureAdls => AZURE_ENV_MAPPING,
            StorageProvider::Gcs => GCS_ENV_MAPPING,
            StorageProvider::Local => &[],
        };

        let mut resolved = explicit_options.clone();
        let mut env_resolved = Vec::new();

        for (option_key, env_var) in env_mapping {
            if !resolved.contains_key(*option_key) {
                if let Ok(val) = std::env::var(env_var) {
                    if !val.is_empty() {
                        resolved.insert((*option_key).to_string(), val);
                        env_resolved.push((*option_key).to_string());
                    }
                }
            }
        }

        ResolvedStorageOptions {
            provider,
            options: resolved,
            env_resolved_keys: env_resolved,
        }
    }

    /// Resolves credentials using a custom environment lookup function.
    ///
    /// Allows injecting env var values without mutating the actual
    /// process environment.
    #[cfg(test)]
    #[must_use]
    pub fn resolve_with_env<F>(
        table_path: &str,
        explicit_options: &HashMap<String, String>,
        env_lookup: F,
    ) -> ResolvedStorageOptions
    where
        F: Fn(&str) -> Option<String>,
    {
        let provider = StorageProvider::detect(table_path);

        if provider == StorageProvider::Local {
            return ResolvedStorageOptions {
                provider,
                options: explicit_options.clone(),
                env_resolved_keys: Vec::new(),
            };
        }

        let env_mapping = match provider {
            StorageProvider::AwsS3 => AWS_ENV_MAPPING,
            StorageProvider::AzureAdls => AZURE_ENV_MAPPING,
            StorageProvider::Gcs => GCS_ENV_MAPPING,
            StorageProvider::Local => &[],
        };

        let mut resolved = explicit_options.clone();
        let mut env_resolved = Vec::new();

        for (option_key, env_var) in env_mapping {
            if !resolved.contains_key(*option_key) {
                if let Some(val) = env_lookup(env_var) {
                    if !val.is_empty() {
                        resolved.insert((*option_key).to_string(), val);
                        env_resolved.push((*option_key).to_string());
                    }
                }
            }
        }

        ResolvedStorageOptions {
            provider,
            options: resolved,
            env_resolved_keys: env_resolved,
        }
    }
}

#[cfg(test)]
mod tests;
