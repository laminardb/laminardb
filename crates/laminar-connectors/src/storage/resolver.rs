//! Cloud storage credential resolver.
//!
//! [`StorageCredentialResolver`] merges explicit `storage.*` config options with
//! environment variable fallbacks, producing a [`ResolvedStorageOptions`] ready
//! for consumption by `object_store` / `deltalake`.
//!
//! Resolution priority chain:
//! 1. Explicit options from SQL `WITH` clause (`storage.*` keys)
//! 2. Environment variables (`AWS_ACCESS_KEY_ID`, etc.)
//! 3. Instance metadata / default credential providers (handled by `object_store`)

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
/// 1. Explicit `storage.*` keys from SQL WITH clause
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
    /// * `explicit_options` - Options from SQL WITH clause (`storage.` prefix already stripped)
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
    /// This is primarily for testing — allows injecting env var values
    /// without mutating the actual process environment.
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
mod tests {
    use super::*;

    fn empty_opts() -> HashMap<String, String> {
        HashMap::new()
    }

    fn env_none(_: &str) -> Option<String> {
        None
    }

    fn aws_env(var: &str) -> Option<String> {
        match var {
            "AWS_ACCESS_KEY_ID" => Some("AKID_FROM_ENV".to_string()),
            "AWS_SECRET_ACCESS_KEY" => Some("SECRET_FROM_ENV".to_string()),
            "AWS_REGION" => Some("us-west-2".to_string()),
            _ => None,
        }
    }

    fn azure_env(var: &str) -> Option<String> {
        match var {
            "AZURE_STORAGE_ACCOUNT_NAME" => Some("myaccount".to_string()),
            "AZURE_STORAGE_ACCOUNT_KEY" => Some("base64key==".to_string()),
            _ => None,
        }
    }

    fn gcs_env(var: &str) -> Option<String> {
        match var {
            "GOOGLE_APPLICATION_CREDENTIALS" => Some("/path/to/creds.json".to_string()),
            _ => None,
        }
    }

    // ── Local path tests ──

    #[test]
    fn test_resolve_local_no_credentials() {
        let resolved =
            StorageCredentialResolver::resolve_with_env("/data/table", &empty_opts(), env_none);
        assert_eq!(resolved.provider, StorageProvider::Local);
        assert!(resolved.options.is_empty());
        assert!(resolved.env_resolved_keys.is_empty());
        assert!(!resolved.has_credentials());
    }

    #[test]
    fn test_resolve_local_preserves_explicit() {
        let mut opts = HashMap::new();
        opts.insert("custom_key".to_string(), "value".to_string());
        let resolved = StorageCredentialResolver::resolve_with_env("/data/table", &opts, env_none);
        assert_eq!(resolved.options.get("custom_key").unwrap(), "value");
    }

    // ── S3 tests ──

    #[test]
    fn test_resolve_s3_explicit_keys() {
        let mut opts = HashMap::new();
        opts.insert("aws_access_key_id".to_string(), "EXPLICIT_KEY".to_string());
        opts.insert(
            "aws_secret_access_key".to_string(),
            "EXPLICIT_SECRET".to_string(),
        );
        opts.insert("aws_region".to_string(), "eu-west-1".to_string());

        let resolved =
            StorageCredentialResolver::resolve_with_env("s3://bucket/path", &opts, aws_env);
        assert_eq!(resolved.provider, StorageProvider::AwsS3);
        assert_eq!(resolved.options["aws_access_key_id"], "EXPLICIT_KEY");
        assert_eq!(resolved.options["aws_secret_access_key"], "EXPLICIT_SECRET");
        assert_eq!(resolved.options["aws_region"], "eu-west-1");
        assert!(resolved.env_resolved_keys.is_empty());
        assert!(resolved.has_credentials());
    }

    #[test]
    fn test_resolve_s3_env_fallback() {
        let resolved =
            StorageCredentialResolver::resolve_with_env("s3://bucket/path", &empty_opts(), aws_env);
        assert_eq!(resolved.options["aws_access_key_id"], "AKID_FROM_ENV");
        assert_eq!(resolved.options["aws_secret_access_key"], "SECRET_FROM_ENV");
        assert_eq!(resolved.options["aws_region"], "us-west-2");
        assert_eq!(resolved.env_resolved_keys.len(), 3);
        assert!(resolved.has_credentials());
    }

    #[test]
    fn test_resolve_s3_explicit_overrides_env() {
        let mut opts = HashMap::new();
        opts.insert("aws_region".to_string(), "ap-southeast-1".to_string());

        let resolved =
            StorageCredentialResolver::resolve_with_env("s3://bucket/path", &opts, aws_env);
        // Explicit region preserved; env keys and secret filled from env.
        assert_eq!(resolved.options["aws_region"], "ap-southeast-1");
        assert_eq!(resolved.options["aws_access_key_id"], "AKID_FROM_ENV");
        assert!(!resolved
            .env_resolved_keys
            .contains(&"aws_region".to_string()));
        assert!(resolved
            .env_resolved_keys
            .contains(&"aws_access_key_id".to_string()));
    }

    #[test]
    fn test_resolve_s3_no_credentials() {
        let resolved = StorageCredentialResolver::resolve_with_env(
            "s3://bucket/path",
            &empty_opts(),
            env_none,
        );
        assert_eq!(resolved.provider, StorageProvider::AwsS3);
        assert!(!resolved.has_credentials());
    }

    #[test]
    fn test_resolve_s3_session_token() {
        let env = |var: &str| -> Option<String> {
            match var {
                "AWS_SESSION_TOKEN" => Some("token123".to_string()),
                _ => None,
            }
        };
        let resolved =
            StorageCredentialResolver::resolve_with_env("s3://bucket/path", &empty_opts(), env);
        assert_eq!(resolved.options["aws_session_token"], "token123");
    }

    #[test]
    fn test_resolve_s3_profile() {
        let mut opts = HashMap::new();
        opts.insert("aws_profile".to_string(), "production".to_string());

        let resolved =
            StorageCredentialResolver::resolve_with_env("s3://bucket/path", &opts, env_none);
        assert!(resolved.has_credentials());
        assert_eq!(resolved.options["aws_profile"], "production");
    }

    #[test]
    fn test_resolve_s3_custom_endpoint() {
        let mut opts = HashMap::new();
        opts.insert(
            "aws_endpoint".to_string(),
            "http://localhost:9000".to_string(),
        );
        opts.insert("aws_s3_allow_unsafe_rename".to_string(), "true".to_string());
        opts.insert("aws_access_key_id".to_string(), "minioadmin".to_string());
        opts.insert(
            "aws_secret_access_key".to_string(),
            "minioadmin".to_string(),
        );

        let resolved =
            StorageCredentialResolver::resolve_with_env("s3://bucket/path", &opts, env_none);
        assert_eq!(resolved.options["aws_endpoint"], "http://localhost:9000");
        assert_eq!(resolved.options["aws_s3_allow_unsafe_rename"], "true");
    }

    // ── Azure tests ──

    #[test]
    fn test_resolve_azure_env_fallback() {
        let resolved = StorageCredentialResolver::resolve_with_env(
            "az://container/path",
            &empty_opts(),
            azure_env,
        );
        assert_eq!(resolved.provider, StorageProvider::AzureAdls);
        assert_eq!(resolved.options["azure_storage_account_name"], "myaccount");
        assert_eq!(resolved.options["azure_storage_account_key"], "base64key==");
        assert!(resolved.has_credentials());
    }

    #[test]
    fn test_resolve_azure_sas_token() {
        let mut opts = HashMap::new();
        opts.insert("azure_storage_account_name".to_string(), "acct".to_string());
        opts.insert(
            "azure_storage_sas_token".to_string(),
            "sv=2021-06&sig=abc".to_string(),
        );

        let resolved = StorageCredentialResolver::resolve_with_env(
            "abfss://container@acct.dfs.core.windows.net/path",
            &opts,
            env_none,
        );
        assert!(resolved.has_credentials());
        assert_eq!(
            resolved.options["azure_storage_sas_token"],
            "sv=2021-06&sig=abc"
        );
    }

    #[test]
    fn test_resolve_azure_client_id() {
        let mut opts = HashMap::new();
        opts.insert("azure_storage_account_name".to_string(), "acct".to_string());
        opts.insert(
            "azure_storage_client_id".to_string(),
            "client-id-123".to_string(),
        );

        let resolved =
            StorageCredentialResolver::resolve_with_env("az://container/path", &opts, env_none);
        assert!(resolved.has_credentials());
    }

    #[test]
    fn test_resolve_azure_no_credentials() {
        let resolved = StorageCredentialResolver::resolve_with_env(
            "az://container/path",
            &empty_opts(),
            env_none,
        );
        assert!(!resolved.has_credentials());
    }

    // ── GCS tests ──

    #[test]
    fn test_resolve_gcs_env_fallback() {
        let resolved =
            StorageCredentialResolver::resolve_with_env("gs://bucket/path", &empty_opts(), gcs_env);
        assert_eq!(resolved.provider, StorageProvider::Gcs);
        assert_eq!(
            resolved.options["google_service_account_path"],
            "/path/to/creds.json"
        );
        assert!(resolved.has_credentials());
    }

    #[test]
    fn test_resolve_gcs_inline_key() {
        let mut opts = HashMap::new();
        opts.insert(
            "google_service_account_key".to_string(),
            r#"{"type":"service_account"}"#.to_string(),
        );

        let resolved =
            StorageCredentialResolver::resolve_with_env("gs://bucket/path", &opts, env_none);
        assert!(resolved.has_credentials());
    }

    #[test]
    fn test_resolve_gcs_no_credentials() {
        let resolved = StorageCredentialResolver::resolve_with_env(
            "gs://bucket/path",
            &empty_opts(),
            env_none,
        );
        assert!(!resolved.has_credentials());
    }

    // ── Env tracking tests ──

    #[test]
    fn test_env_resolved_keys_tracked() {
        let resolved =
            StorageCredentialResolver::resolve_with_env("s3://bucket/path", &empty_opts(), aws_env);
        assert!(resolved
            .env_resolved_keys
            .contains(&"aws_access_key_id".to_string()));
        assert!(resolved
            .env_resolved_keys
            .contains(&"aws_secret_access_key".to_string()));
        assert!(resolved
            .env_resolved_keys
            .contains(&"aws_region".to_string()));
    }

    #[test]
    fn test_empty_env_var_not_used() {
        let env = |var: &str| -> Option<String> {
            match var {
                "AWS_REGION" => Some(String::new()),
                _ => None,
            }
        };
        let resolved =
            StorageCredentialResolver::resolve_with_env("s3://bucket/path", &empty_opts(), env);
        assert!(!resolved.options.contains_key("aws_region"));
    }

    #[test]
    fn test_s3a_resolves_as_s3() {
        let resolved = StorageCredentialResolver::resolve_with_env(
            "s3a://bucket/path",
            &empty_opts(),
            aws_env,
        );
        assert_eq!(resolved.provider, StorageProvider::AwsS3);
        assert!(resolved.has_credentials());
    }
}
