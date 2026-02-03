//! Per-cloud-provider configuration validation.
//!
//! [`CloudConfigValidator`] checks [`ResolvedStorageOptions`] for missing
//! or invalid credentials at connector `open()` time, producing clear,
//! actionable error messages that include both the config key name and
//! the fallback environment variable.

use super::provider::StorageProvider;
use super::resolver::ResolvedStorageOptions;

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
        match resolved.provider {
            StorageProvider::AwsS3 => Self::validate_s3(resolved),
            StorageProvider::AzureAdls => Self::validate_azure(resolved),
            StorageProvider::Gcs => Self::validate_gcs(resolved),
            StorageProvider::Local => CloudValidationResult {
                errors: Vec::new(),
                warnings: Vec::new(),
            },
        }
    }

    fn validate_s3(resolved: &ResolvedStorageOptions) -> CloudValidationResult {
        let mut errors = Vec::new();
        let mut warnings = Vec::new();

        // Region is required (no instance metadata fallback for region in object_store).
        if !resolved.options.contains_key("aws_region") {
            errors.push(CloudValidationError {
                key: "aws_region".into(),
                env_var: Some("AWS_REGION".into()),
                message: "S3 paths require 'storage.aws_region' in config \
                          or AWS_REGION environment variable"
                    .into(),
            });
        }

        // If access key is provided, secret key must also be provided.
        if resolved.options.contains_key("aws_access_key_id")
            && !resolved.options.contains_key("aws_secret_access_key")
        {
            errors.push(CloudValidationError {
                key: "aws_secret_access_key".into(),
                env_var: Some("AWS_SECRET_ACCESS_KEY".into()),
                message: "'storage.aws_access_key_id' provided without \
                          'storage.aws_secret_access_key'"
                    .into(),
            });
        }

        // If secret key is provided, access key must also be provided.
        if resolved.options.contains_key("aws_secret_access_key")
            && !resolved.options.contains_key("aws_access_key_id")
        {
            errors.push(CloudValidationError {
                key: "aws_access_key_id".into(),
                env_var: Some("AWS_ACCESS_KEY_ID".into()),
                message: "'storage.aws_secret_access_key' provided without \
                          'storage.aws_access_key_id'"
                    .into(),
            });
        }

        // Warn if no credentials at all (may use IAM role / instance profile).
        if !resolved.has_credentials() {
            warnings.push(CloudValidationWarning {
                key: "aws_access_key_id".into(),
                message: "No AWS credentials found in config or environment. \
                          Will fall back to instance metadata (IAM role). \
                          Set 'storage.aws_access_key_id' / \
                          'storage.aws_secret_access_key' or AWS_ACCESS_KEY_ID / \
                          AWS_SECRET_ACCESS_KEY if needed."
                    .into(),
            });
        }

        CloudValidationResult { errors, warnings }
    }

    fn validate_azure(resolved: &ResolvedStorageOptions) -> CloudValidationResult {
        let mut errors = Vec::new();
        let mut warnings = Vec::new();

        // Account name is always required for Azure.
        if !resolved.options.contains_key("azure_storage_account_name") {
            errors.push(CloudValidationError {
                key: "azure_storage_account_name".into(),
                env_var: Some("AZURE_STORAGE_ACCOUNT_NAME".into()),
                message: "Azure paths require 'storage.azure_storage_account_name' \
                          in config or AZURE_STORAGE_ACCOUNT_NAME environment variable"
                    .into(),
            });
        }

        // Warn if no credentials (may use Managed Identity).
        if !resolved.has_credentials() {
            warnings.push(CloudValidationWarning {
                key: "azure_storage_account_key".into(),
                message: "No Azure credentials found in config or environment. \
                          Will fall back to Managed Identity. \
                          Set 'storage.azure_storage_account_key' / \
                          AZURE_STORAGE_ACCOUNT_KEY, or \
                          'storage.azure_storage_sas_token' / \
                          AZURE_STORAGE_SAS_TOKEN if needed."
                    .into(),
            });
        }

        CloudValidationResult { errors, warnings }
    }

    fn validate_gcs(resolved: &ResolvedStorageOptions) -> CloudValidationResult {
        let mut warnings = Vec::new();

        // GCS credentials are warning-only (Application Default Credentials / Workload Identity).
        if !resolved.has_credentials() {
            warnings.push(CloudValidationWarning {
                key: "google_service_account_path".into(),
                message: "No GCS credentials found in config or environment. \
                          Will fall back to Application Default Credentials / \
                          Workload Identity. Set \
                          'storage.google_service_account_path' / \
                          GOOGLE_APPLICATION_CREDENTIALS if needed."
                    .into(),
            });
        }

        CloudValidationResult {
            errors: Vec::new(),
            warnings,
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use super::*;

    fn make_resolved(provider: StorageProvider, keys: &[(&str, &str)]) -> ResolvedStorageOptions {
        let mut options = HashMap::new();
        for (k, v) in keys {
            options.insert((*k).to_string(), (*v).to_string());
        }
        ResolvedStorageOptions {
            provider,
            options,
            env_resolved_keys: Vec::new(),
        }
    }

    // ── S3 validation ──

    #[test]
    fn test_validate_s3_all_present() {
        let resolved = make_resolved(
            StorageProvider::AwsS3,
            &[
                ("aws_access_key_id", "AKID"),
                ("aws_secret_access_key", "SECRET"),
                ("aws_region", "us-east-1"),
            ],
        );
        let result = CloudConfigValidator::validate(&resolved);
        assert!(result.is_valid());
        assert!(result.warnings.is_empty());
    }

    #[test]
    fn test_validate_s3_missing_region() {
        let resolved = make_resolved(
            StorageProvider::AwsS3,
            &[
                ("aws_access_key_id", "AKID"),
                ("aws_secret_access_key", "SECRET"),
            ],
        );
        let result = CloudConfigValidator::validate(&resolved);
        assert!(!result.is_valid());
        assert!(result.errors.iter().any(|e| e.key == "aws_region"));
        assert_eq!(result.errors[0].env_var.as_deref(), Some("AWS_REGION"));
    }

    #[test]
    fn test_validate_s3_missing_credentials_warns() {
        let resolved = make_resolved(StorageProvider::AwsS3, &[("aws_region", "us-east-1")]);
        let result = CloudConfigValidator::validate(&resolved);
        assert!(result.is_valid()); // Warning, not error.
        assert!(!result.warnings.is_empty());
    }

    #[test]
    fn test_validate_s3_access_key_without_secret() {
        let resolved = make_resolved(
            StorageProvider::AwsS3,
            &[("aws_access_key_id", "AKID"), ("aws_region", "us-east-1")],
        );
        let result = CloudConfigValidator::validate(&resolved);
        assert!(!result.is_valid());
        assert!(result
            .errors
            .iter()
            .any(|e| e.key == "aws_secret_access_key"));
    }

    #[test]
    fn test_validate_s3_secret_key_without_access() {
        let resolved = make_resolved(
            StorageProvider::AwsS3,
            &[
                ("aws_secret_access_key", "SECRET"),
                ("aws_region", "us-east-1"),
            ],
        );
        let result = CloudConfigValidator::validate(&resolved);
        assert!(!result.is_valid());
        assert!(result.errors.iter().any(|e| e.key == "aws_access_key_id"));
    }

    #[test]
    fn test_validate_s3_profile_sufficient() {
        let resolved = make_resolved(
            StorageProvider::AwsS3,
            &[("aws_profile", "production"), ("aws_region", "us-east-1")],
        );
        let result = CloudConfigValidator::validate(&resolved);
        assert!(result.is_valid());
        assert!(result.warnings.is_empty()); // profile counts as credentials
    }

    // ── Azure validation ──

    #[test]
    fn test_validate_azure_all_present() {
        let resolved = make_resolved(
            StorageProvider::AzureAdls,
            &[
                ("azure_storage_account_name", "myaccount"),
                ("azure_storage_account_key", "base64key=="),
            ],
        );
        let result = CloudConfigValidator::validate(&resolved);
        assert!(result.is_valid());
        assert!(result.warnings.is_empty());
    }

    #[test]
    fn test_validate_azure_missing_account_name() {
        let resolved = make_resolved(
            StorageProvider::AzureAdls,
            &[("azure_storage_account_key", "base64key==")],
        );
        let result = CloudConfigValidator::validate(&resolved);
        assert!(!result.is_valid());
        assert!(result
            .errors
            .iter()
            .any(|e| e.key == "azure_storage_account_name"));
    }

    #[test]
    fn test_validate_azure_sas_token_sufficient() {
        let resolved = make_resolved(
            StorageProvider::AzureAdls,
            &[
                ("azure_storage_account_name", "myaccount"),
                ("azure_storage_sas_token", "sv=2021-06&sig=abc"),
            ],
        );
        let result = CloudConfigValidator::validate(&resolved);
        assert!(result.is_valid());
        assert!(result.warnings.is_empty());
    }

    #[test]
    fn test_validate_azure_client_id_sufficient() {
        let resolved = make_resolved(
            StorageProvider::AzureAdls,
            &[
                ("azure_storage_account_name", "myaccount"),
                ("azure_storage_client_id", "client-123"),
            ],
        );
        let result = CloudConfigValidator::validate(&resolved);
        assert!(result.is_valid());
    }

    #[test]
    fn test_validate_azure_missing_credentials_warns() {
        let resolved = make_resolved(
            StorageProvider::AzureAdls,
            &[("azure_storage_account_name", "myaccount")],
        );
        let result = CloudConfigValidator::validate(&resolved);
        assert!(result.is_valid());
        assert!(!result.warnings.is_empty());
    }

    // ── GCS validation ──

    #[test]
    fn test_validate_gcs_all_present() {
        let resolved = make_resolved(
            StorageProvider::Gcs,
            &[("google_service_account_path", "/path/to/creds.json")],
        );
        let result = CloudConfigValidator::validate(&resolved);
        assert!(result.is_valid());
        assert!(result.warnings.is_empty());
    }

    #[test]
    fn test_validate_gcs_inline_key_sufficient() {
        let resolved = make_resolved(
            StorageProvider::Gcs,
            &[(
                "google_service_account_key",
                "{\"type\":\"service_account\"}",
            )],
        );
        let result = CloudConfigValidator::validate(&resolved);
        assert!(result.is_valid());
        assert!(result.warnings.is_empty());
    }

    #[test]
    fn test_validate_gcs_missing_credentials_warns() {
        let resolved = make_resolved(StorageProvider::Gcs, &[]);
        let result = CloudConfigValidator::validate(&resolved);
        assert!(result.is_valid()); // Warning only.
        assert!(!result.warnings.is_empty());
    }

    // ── Local validation ──

    #[test]
    fn test_validate_local_always_valid() {
        let resolved = make_resolved(StorageProvider::Local, &[]);
        let result = CloudConfigValidator::validate(&resolved);
        assert!(result.is_valid());
        assert!(result.warnings.is_empty());
    }

    // ── Utility tests ──

    #[test]
    fn test_error_message_formatting() {
        let resolved = make_resolved(StorageProvider::AwsS3, &[]);
        let result = CloudConfigValidator::validate(&resolved);
        assert!(!result.is_valid());
        let msg = result.error_message();
        assert!(msg.contains("aws_region"));
    }

    #[test]
    fn test_error_includes_env_var_hint() {
        let resolved = make_resolved(StorageProvider::AwsS3, &[]);
        let result = CloudConfigValidator::validate(&resolved);
        let region_err = result
            .errors
            .iter()
            .find(|e| e.key == "aws_region")
            .unwrap();
        assert_eq!(region_err.env_var.as_deref(), Some("AWS_REGION"));
        assert!(region_err.message.contains("AWS_REGION"));
    }
}
