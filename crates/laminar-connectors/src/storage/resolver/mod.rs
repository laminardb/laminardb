//! Cloud storage credential resolver.
//!
//! [`StorageCredentialResolver`] retains explicit `storage.*` config options and
//! records non-secret information about ambient credential sources. Provider
//! libraries read the environment themselves when constructing a client, so
//! rotating tokens are never copied into connector configuration.
//!
//! Resolution priority chain:
//! 1. Explicit connector options (`storage.*` keys)
//! 2. Explicitly selected profiles or credential sources
//! 3. Provider environment and default credential chains (handled downstream)
#![allow(clippy::disallowed_types)] // cold path: storage configuration

use std::collections::HashMap;
use std::fmt;

use super::provider::StorageProvider;
use laminar_core::storage_auth::classify_storage_auth_source;
pub use laminar_core::storage_auth::AuthSource;
use laminar_core::storage_location::StorageEndpointClass;

/// AWS S3 environment variable fallbacks.
///
/// Maps option key (as used by `object_store`/`deltalake`) to env var name.
const AWS_ENV_MAPPING: &[(&str, &str)] = &[
    ("aws_access_key_id", "AWS_ACCESS_KEY_ID"),
    ("aws_secret_access_key", "AWS_SECRET_ACCESS_KEY"),
    ("aws_region", "AWS_REGION"),
    ("aws_default_region", "AWS_DEFAULT_REGION"),
    ("aws_session_token", "AWS_SESSION_TOKEN"),
    ("aws_endpoint", "AWS_ENDPOINT_URL"),
    ("aws_endpoint", "AWS_ENDPOINT"),
    ("aws_endpoint_url_s3", "AWS_ENDPOINT_URL_S3"),
    ("aws_profile", "AWS_PROFILE"),
    ("aws_web_identity_token_file", "AWS_WEB_IDENTITY_TOKEN_FILE"),
    ("aws_role_arn", "AWS_ROLE_ARN"),
    ("aws_role_session_name", "AWS_ROLE_SESSION_NAME"),
    (
        "aws_container_credentials_relative_uri",
        "AWS_CONTAINER_CREDENTIALS_RELATIVE_URI",
    ),
    (
        "aws_container_credentials_full_uri",
        "AWS_CONTAINER_CREDENTIALS_FULL_URI",
    ),
    (
        "aws_container_authorization_token_file",
        "AWS_CONTAINER_AUTHORIZATION_TOKEN_FILE",
    ),
    (
        "aws_virtual_hosted_style_request",
        "AWS_VIRTUAL_HOSTED_STYLE_REQUEST",
    ),
    ("aws_allow_http", "AWS_ALLOW_HTTP"),
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
    ("azure_federated_token_file", "AZURE_FEDERATED_TOKEN_FILE"),
    ("azure_storage_authority_host", "AZURE_AUTHORITY_HOST"),
    ("azure_storage_endpoint", "AZURE_STORAGE_ENDPOINT"),
    ("azure_allow_http", "AZURE_ALLOW_HTTP"),
    ("azure_msi_endpoint", "IDENTITY_ENDPOINT"),
    ("azure_use_azure_cli", "AZURE_USE_AZURE_CLI"),
];

/// GCS environment variable fallbacks.
const GCS_ENV_MAPPING: &[(&str, &str)] = &[
    ("google_service_account_path", "SERVICE_ACCOUNT"),
    ("google_service_account_path", "GOOGLE_SERVICE_ACCOUNT"),
    ("google_service_account_path", "GOOGLE_SERVICE_ACCOUNT_PATH"),
    ("google_service_account_key", "GOOGLE_SERVICE_ACCOUNT_KEY"),
    (
        "google_application_credentials",
        "GOOGLE_APPLICATION_CREDENTIALS",
    ),
    ("google_base_url", "GOOGLE_BASE_URL"),
    ("google_base_url", "GOOGLE_ENDPOINT_URL"),
    ("google_allow_http", "GOOGLE_ALLOW_HTTP"),
];

/// Resolved storage credentials ready for `object_store` / `deltalake`.
#[derive(Clone)]
pub struct ResolvedStorageOptions {
    /// Detected cloud provider.
    pub provider: StorageProvider,
    /// Explicit and URL-derived options. Keys match what
    /// `deltalake`/`object_store` expect.
    pub options: HashMap<String, String>,
    /// Canonical keys whose non-empty environment sources were observed.
    ///
    /// Credential values are deliberately not retained. The effective endpoint
    /// URL is retained only so the same validation applies to explicit and
    /// environment-selected compatibility endpoints.
    pub env_resolved_keys: Vec<String>,
    /// Non-secret credential mechanism selected by resolution.
    pub auth_source: AuthSource,
    /// Effective native, compatibility, emulator, or local endpoint class.
    pub endpoint_class: StorageEndpointClass,
    /// Whether connector options or provider environment selected an endpoint override.
    pub endpoint_override_configured: bool,
}

impl ResolvedStorageOptions {
    /// Whether concrete credential material or an explicit credential source was selected.
    ///
    /// A `false` result does not mean credentials are missing: ambient metadata
    /// and downstream default chains intentionally return `false`.
    #[must_use]
    pub fn has_credentials(&self) -> bool {
        matches!(
            self.auth_source,
            AuthSource::ExplicitStatic
                | AuthSource::ExplicitToken
                | AuthSource::EnvironmentStatic
                | AuthSource::EnvironmentToken
                | AuthSource::Profile
                | AuthSource::WebIdentity
                | AuthSource::WorkloadIdentity
                | AuthSource::AzureCli
        )
    }

    /// Effective endpoint class without exposing the endpoint or location.
    #[must_use]
    pub fn endpoint_class(&self) -> StorageEndpointClass {
        self.endpoint_class
    }
}

impl fmt::Debug for ResolvedStorageOptions {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut option_keys = self.options.keys().collect::<Vec<_>>();
        option_keys.sort_unstable();
        let mut environment_keys = self.env_resolved_keys.iter().collect::<Vec<_>>();
        environment_keys.sort_unstable();
        formatter
            .debug_struct("ResolvedStorageOptions")
            .field("provider", &self.provider)
            .field("auth_source", &self.auth_source)
            .field("endpoint_class", &self.endpoint_class)
            .field(
                "endpoint_override_configured",
                &self.endpoint_override_configured,
            )
            .field("option_keys", &option_keys)
            .field("env_resolved_keys", &environment_keys)
            .finish()
    }
}

/// Storage credential resolver.
///
/// Resolves credentials by priority chain:
/// 1. Explicit `storage.*` connector options
/// 2. Explicitly selected profiles or credential sources
/// 3. Provider environment and default credential chains (handled downstream)
pub struct StorageCredentialResolver;

impl StorageCredentialResolver {
    /// Resolves storage credentials for the given table path.
    ///
    /// Retains explicit options and classifies environment/default-chain
    /// sources appropriate for the detected cloud provider.
    ///
    /// # Arguments
    ///
    /// * `table_path` - URI of the table (`s3://`, `az://`, `gs://`, or local path)
    /// * `explicit_options` - Connector options (`storage.` prefix already stripped)
    ///
    /// # Returns
    ///
    /// [`ResolvedStorageOptions`] with explicit options and ambient-source metadata.
    #[must_use]
    pub fn resolve(
        table_path: &str,
        explicit_options: &HashMap<String, String>,
    ) -> ResolvedStorageOptions {
        Self::resolve_from(table_path, explicit_options, &|name| {
            std::env::var(name).ok()
        })
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
        Self::resolve_from(table_path, explicit_options, &env_lookup)
    }

    fn resolve_from<F>(
        table_path: &str,
        explicit_options: &HashMap<String, String>,
        env_lookup: &F,
    ) -> ResolvedStorageOptions
    where
        F: Fn(&str) -> Option<String>,
    {
        let parsed_location =
            laminar_core::storage_location::StorageLocation::parse(table_path).ok();
        let provider = parsed_location.as_ref().map_or_else(
            || StorageProvider::detect(table_path),
            |location| location.provider,
        );

        if provider == StorageProvider::Local {
            return ResolvedStorageOptions {
                provider,
                options: explicit_options.clone(),
                env_resolved_keys: Vec::new(),
                auth_source: AuthSource::Anonymous,
                endpoint_class: StorageEndpointClass::Local,
                endpoint_override_configured: false,
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
        let explicit_endpoint = explicit_options
            .iter()
            .any(|(key, value)| is_endpoint_key(key) && !value.trim().is_empty());

        for (option_key, env_var) in env_mapping {
            if !resolved.contains_key(*option_key) {
                if let Some(val) = env_lookup(env_var) {
                    if effective_environment_value(option_key, &val) {
                        if explicit_endpoint && is_endpoint_key(option_key) {
                            continue;
                        }
                        let canonical_key = (*option_key).to_string();
                        if !env_resolved.contains(&canonical_key) {
                            env_resolved.push(canonical_key.clone());
                        }
                        if is_endpoint_key(option_key)
                            && !explicit_endpoint
                            && !resolved.keys().any(|key| is_endpoint_key(key))
                        {
                            resolved.insert(canonical_key, val);
                        }
                    }
                }
            }
        }

        let endpoint_override_configured =
            explicit_endpoint || env_resolved.iter().any(|key| is_endpoint_key(key));
        let endpoint_class = resolved_endpoint_class(
            provider,
            parsed_location.as_ref(),
            endpoint_override_configured,
        );

        if let Some(Ok(adapted)) = parsed_location.as_ref().map(|location| {
            location.adapt(laminar_core::storage_location::StorageConsumer::ObjectStore)
        }) {
            for (key, value) in adapted.derived_options {
                resolved.entry(key).or_insert(value);
            }
        }

        let auth_source = classify_storage_auth_source(provider, explicit_options, env_lookup);

        ResolvedStorageOptions {
            provider,
            options: resolved,
            env_resolved_keys: env_resolved,
            auth_source,
            endpoint_class,
            endpoint_override_configured,
        }
    }
}

fn resolved_endpoint_class(
    provider: StorageProvider,
    location: Option<&laminar_core::storage_location::StorageLocation>,
    has_override: bool,
) -> StorageEndpointClass {
    if has_override {
        return match provider {
            StorageProvider::AwsS3 => StorageEndpointClass::S3Compatible,
            StorageProvider::AzureAdls | StorageProvider::Gcs => {
                StorageEndpointClass::CustomOrEmulator
            }
            StorageProvider::Local => StorageEndpointClass::Local,
        };
    }
    location.map_or(StorageEndpointClass::Native, |location| {
        location.endpoint_class()
    })
}

fn effective_environment_value(option_key: &str, value: &str) -> bool {
    if value.trim().is_empty() {
        return false;
    }
    if option_key.ends_with("allow_http") {
        return value.trim().eq_ignore_ascii_case("true");
    }
    true
}

fn is_endpoint_key(key: &str) -> bool {
    matches!(
        key.to_ascii_lowercase().as_str(),
        "endpoint"
            | "endpoint_url"
            | "aws_endpoint"
            | "aws_endpoint_url"
            | "aws_endpoint_url_s3"
            | "azure_endpoint"
            | "azure_storage_endpoint"
            | "base_url"
            | "google_base_url"
            | "google_service_path"
            | "gcs.service.path"
    )
}

#[cfg(test)]
mod tests;
