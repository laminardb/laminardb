//! Per-cloud-provider configuration validation.
//!
//! [`CloudConfigValidator`] checks [`ResolvedStorageOptions`] for missing
//! or invalid credentials at connector `open()` time, producing clear,
//! actionable error messages that include both the config key name and
//! the fallback environment variable.

use super::provider::StorageProvider;
use super::resolver::{AuthSource, ResolvedStorageOptions};

/// Result of cloud configuration validation.
#[derive(Debug, Clone)]
pub struct CloudValidationResult {
    /// Hard errors that prevent the connector from opening.
    pub errors: Vec<CloudValidationError>,
    /// Soft warnings (may still work with instance metadata / default creds).
    pub warnings: Vec<CloudValidationWarning>,
}

impl CloudValidationResult {
    /// Returns true if there are no hard errors.
    #[must_use]
    pub fn is_valid(&self) -> bool {
        self.errors.is_empty()
    }

    /// Formats all errors into a single string for `ConnectorError`.
    #[must_use]
    pub fn error_message(&self) -> String {
        self.errors
            .iter()
            .map(|e| e.message.as_str())
            .collect::<Vec<_>>()
            .join("; ")
    }
}

/// A hard validation error.
#[derive(Debug, Clone)]
pub struct CloudValidationError {
    /// The missing or invalid configuration key.
    pub key: String,
    /// The fallback environment variable (if applicable).
    pub env_var: Option<String>,
    /// Human-readable error message.
    pub message: String,
}

/// A soft validation warning.
#[derive(Debug, Clone)]
pub struct CloudValidationWarning {
    /// The configuration key this warning relates to.
    pub key: String,
    /// Human-readable warning message.
    pub message: String,
}

/// Validates resolved storage options for a given provider.
pub struct CloudConfigValidator;

impl CloudConfigValidator {
    /// Validates the resolved storage options for the detected provider.
    ///
    /// Returns a [`CloudValidationResult`] with any errors or warnings.
    /// Hard errors indicate the connector cannot open. Warnings indicate
    /// missing credentials that may still be resolved by instance metadata
    /// or default credential providers.
    #[must_use]
    pub fn validate(resolved: &ResolvedStorageOptions) -> CloudValidationResult {
        let mut result = match resolved.provider {
            StorageProvider::AwsS3 => Self::validate_s3(resolved),
            StorageProvider::AzureAdls => Self::validate_azure(resolved),
            StorageProvider::Gcs => Self::validate_gcs(resolved),
            StorageProvider::Local => CloudValidationResult {
                errors: Vec::new(),
                warnings: Vec::new(),
            },
        };
        validate_endpoint_options(resolved, &mut result.errors);
        result
    }

    fn validate_s3(resolved: &ResolvedStorageOptions) -> CloudValidationResult {
        let mut errors = Vec::new();
        let mut warnings = Vec::new();

        if !has_any(resolved, &["aws_region", "aws_default_region", "region"]) {
            warnings.push(CloudValidationWarning {
                key: "aws_region".into(),
                message: "No explicit AWS region is configured; the downstream AWS region chain remains active"
                    .into(),
            });
        }

        // If access key is provided, secret key must also be provided.
        let access_key = has_any(resolved, &["aws_access_key_id", "access_key_id"]);
        let secret_key = has_any(resolved, &["aws_secret_access_key", "secret_access_key"]);
        if access_key && !secret_key {
            errors.push(CloudValidationError {
                key: "aws_secret_access_key".into(),
                env_var: Some("AWS_SECRET_ACCESS_KEY".into()),
                message: "'storage.aws_access_key_id' provided without \
                          'storage.aws_secret_access_key'"
                    .into(),
            });
        }

        // If secret key is provided, access key must also be provided.
        if secret_key && !access_key {
            errors.push(CloudValidationError {
                key: "aws_access_key_id".into(),
                env_var: Some("AWS_ACCESS_KEY_ID".into()),
                message: "'storage.aws_secret_access_key' provided without \
                          'storage.aws_access_key_id'"
                    .into(),
            });
        }

        let web_identity = has_any(
            resolved,
            &["aws_web_identity_token_file", "web_identity_token_file"],
        );
        let role = has_any(resolved, &["aws_role_arn", "role_arn"]);
        if web_identity != role {
            let key = if web_identity {
                "aws_role_arn"
            } else {
                "aws_web_identity_token_file"
            };
            errors.push(CloudValidationError {
                key: key.into(),
                env_var: Some(if web_identity {
                    "AWS_ROLE_ARN".into()
                } else {
                    "AWS_WEB_IDENTITY_TOKEN_FILE".into()
                }),
                message: format!("AWS web identity configuration is missing '{key}'"),
            });
        }

        CloudValidationResult { errors, warnings }
    }

    fn validate_azure(resolved: &ResolvedStorageOptions) -> CloudValidationResult {
        let mut errors = Vec::new();
        let warnings = Vec::new();

        // Account name is always required for Azure.
        if !has_any(
            resolved,
            &["azure_storage_account_name", "azure_account_name"],
        ) {
            errors.push(CloudValidationError {
                key: "azure_storage_account_name".into(),
                env_var: Some("AZURE_STORAGE_ACCOUNT_NAME".into()),
                message: "Azure paths require 'storage.azure_storage_account_name' \
                          in config or AZURE_STORAGE_ACCOUNT_NAME environment variable"
                    .into(),
            });
        }

        let client_secret = has_any(
            resolved,
            &[
                "azure_storage_client_secret",
                "azure_client_secret",
                "client_secret",
            ],
        );
        let client_id = has_any(
            resolved,
            &["azure_storage_client_id", "azure_client_id", "client_id"],
        );
        let tenant_id = has_any(
            resolved,
            &["azure_storage_tenant_id", "azure_tenant_id", "tenant_id"],
        );
        if client_secret && !client_id {
            errors.push(missing_field(
                "azure_storage_client_id",
                "AZURE_CLIENT_ID",
                "Azure client-secret authentication",
            ));
        }
        if client_secret && !tenant_id {
            errors.push(missing_field(
                "azure_storage_tenant_id",
                "AZURE_TENANT_ID",
                "Azure client-secret authentication",
            ));
        }
        if resolved.auth_source == AuthSource::WorkloadIdentity {
            if !client_id {
                errors.push(missing_field(
                    "azure_storage_client_id",
                    "AZURE_CLIENT_ID",
                    "Azure workload identity",
                ));
            }
            if !tenant_id {
                errors.push(missing_field(
                    "azure_storage_tenant_id",
                    "AZURE_TENANT_ID",
                    "Azure workload identity",
                ));
            }
        }

        CloudValidationResult { errors, warnings }
    }

    fn validate_gcs(resolved: &ResolvedStorageOptions) -> CloudValidationResult {
        let mut errors = Vec::new();
        if has_any(resolved, &["gcs.token", "google_token"]) {
            errors.push(CloudValidationError {
                key: "gcs.token".into(),
                env_var: None,
                message: "the pinned Delta/object_store GCS backend does not accept a direct access token; configure a service-account path/key or supported Application Default Credentials"
                    .into(),
            });
        }
        CloudValidationResult {
            errors,
            warnings: Vec::new(),
        }
    }
}

fn has_any(resolved: &ResolvedStorageOptions, keys: &[&str]) -> bool {
    resolved.options.iter().any(|(candidate, value)| {
        keys.iter().any(|key| candidate.eq_ignore_ascii_case(key)) && !value.trim().is_empty()
    }) || resolved
        .env_resolved_keys
        .iter()
        .any(|candidate| keys.iter().any(|key| candidate.eq_ignore_ascii_case(key)))
}

fn missing_field(key: &str, env_var: &str, context: &str) -> CloudValidationError {
    CloudValidationError {
        key: key.into(),
        env_var: Some(env_var.into()),
        message: format!("{context} is missing '{key}'"),
    }
}

fn validate_endpoint_options(
    resolved: &ResolvedStorageOptions,
    errors: &mut Vec<CloudValidationError>,
) {
    let endpoints = resolved.options.iter().filter(|(key, value)| {
        matches!(
            key.to_ascii_lowercase().as_str(),
            "aws_endpoint"
                | "aws_endpoint_url"
                | "aws_endpoint_url_s3"
                | "azure_storage_endpoint"
                | "azure_endpoint"
                | "google_base_url"
                | "base_url"
        ) && !value.trim().is_empty()
    });
    if resolved.provider != StorageProvider::Local
        && resolved.auth_source == AuthSource::Anonymous
        && !resolved.endpoint_override_configured
    {
        errors.push(CloudValidationError {
            key: "storage.endpoint".into(),
            env_var: None,
            message:
                "anonymous object-store access requires an explicit compatibility or test endpoint"
                    .into(),
        });
    }
    for (key, value) in endpoints {
        let parsed = match url::Url::parse(value) {
            Ok(parsed)
                if matches!(parsed.scheme(), "http" | "https")
                    && parsed.host_str().is_some()
                    && parsed.username().is_empty()
                    && parsed.password().is_none()
                    && parsed.query().is_none()
                    && parsed.fragment().is_none() =>
            {
                parsed
            }
            _ => {
                errors.push(CloudValidationError {
                    key: key.clone(),
                    env_var: None,
                    message: format!(
                        "storage endpoint option '{key}' must be an HTTP(S) URL without credentials, query parameters, or fragments"
                    ),
                });
                continue;
            }
        };
        if parsed.scheme() == "http"
            && !has_true(
                resolved,
                &[
                    "aws_allow_http",
                    "allow_http",
                    "google_allow_http",
                    "azure_storage_use_emulator",
                    "use_emulator",
                ],
            )
        {
            errors.push(CloudValidationError {
                key: "allow_http".into(),
                env_var: None,
                message:
                    "an HTTP object-store endpoint requires an explicit allow-http or emulator option"
                        .into(),
            });
        }
    }
}

fn has_true(resolved: &ResolvedStorageOptions, keys: &[&str]) -> bool {
    resolved.options.iter().any(|(candidate, value)| {
        keys.iter().any(|key| candidate.eq_ignore_ascii_case(key))
            && value.trim().eq_ignore_ascii_case("true")
    }) || resolved
        .env_resolved_keys
        .iter()
        .any(|candidate| keys.iter().any(|key| candidate.eq_ignore_ascii_case(key)))
}

#[cfg(test)]
#[allow(clippy::disallowed_types)] // cold path: storage configuration
mod tests;
