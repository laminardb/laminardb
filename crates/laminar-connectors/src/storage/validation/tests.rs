use std::collections::HashMap;

use super::*;
use crate::storage::StorageCredentialResolver;

fn make_resolved(provider: StorageProvider, keys: &[(&str, &str)]) -> ResolvedStorageOptions {
    let mut options = HashMap::new();
    for (k, v) in keys {
        options.insert((*k).to_string(), (*v).to_string());
    }
    let location = match provider {
        StorageProvider::AwsS3 => "s3://bucket/path",
        StorageProvider::AzureAdls => "az://container/path",
        StorageProvider::Gcs => "gs://bucket/path",
        StorageProvider::Local => "file:///tmp/path",
    };
    StorageCredentialResolver::resolve_with_env(location, &options, |_| None)
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
    assert!(result.is_valid());
    assert!(result
        .warnings
        .iter()
        .any(|warning| warning.key == "aws_region"));
}

#[test]
fn test_validate_s3_missing_credentials_warns() {
    let resolved = make_resolved(StorageProvider::AwsS3, &[("aws_region", "us-east-1")]);
    let result = CloudConfigValidator::validate(&resolved);
    assert!(result.is_valid()); // Warning, not error.
    assert!(result.warnings.is_empty());
    assert_eq!(resolved.auth_source, AuthSource::DownstreamDefault);
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
fn ambient_partial_s3_credentials_name_only_the_missing_field() {
    let resolved =
        StorageCredentialResolver::resolve_with_env("s3://bucket/path", &HashMap::new(), |name| {
            (name == "AWS_ACCESS_KEY_ID").then(|| "ambient-id".to_string())
        });
    let result = CloudConfigValidator::validate(&resolved);
    assert!(!result.is_valid());
    assert_eq!(result.errors[0].key, "aws_secret_access_key");
    assert!(!result.error_message().contains("ambient-id"));
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
fn ambient_azure_account_and_key_are_validated_without_being_copied() {
    let resolved = StorageCredentialResolver::resolve_with_env(
        "az://container/path",
        &HashMap::new(),
        |name| match name {
            "AZURE_STORAGE_ACCOUNT_NAME" => Some("account-reference".to_string()),
            "AZURE_STORAGE_ACCOUNT_KEY" => Some("ambient-secret".to_string()),
            _ => None,
        },
    );
    assert!(CloudConfigValidator::validate(&resolved).is_valid());
    assert!(resolved.options.is_empty());
    assert!(!format!("{resolved:?}").contains("ambient-secret"));
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
    assert!(result.warnings.is_empty());
    assert_eq!(resolved.auth_source, AuthSource::ManagedIdentityOrMetadata);
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
    assert!(result.warnings.is_empty());
    assert_eq!(resolved.auth_source, AuthSource::ApplicationDefault);
}

#[test]
fn direct_gcs_access_token_is_rejected_by_delta_storage_validation() {
    let resolved = make_resolved(StorageProvider::Gcs, &[("gcs.token", "short-lived-token")]);
    let result = CloudConfigValidator::validate(&resolved);
    assert!(!result.is_valid());
    assert_eq!(result.errors[0].key, "gcs.token");
    assert!(!result.error_message().contains("short-lived-token"));
}

#[test]
fn http_endpoint_requires_allow_http_to_be_true() {
    let denied = make_resolved(
        StorageProvider::AwsS3,
        &[
            ("aws_endpoint", "http://minio.invalid:9000"),
            ("aws_allow_http", "false"),
        ],
    );
    let result = CloudConfigValidator::validate(&denied);
    assert!(!result.is_valid());
    assert!(result.errors.iter().any(|error| error.key == "allow_http"));

    let allowed = make_resolved(
        StorageProvider::AwsS3,
        &[
            ("aws_endpoint", "http://minio.invalid:9000"),
            ("aws_allow_http", "true"),
        ],
    );
    assert!(CloudConfigValidator::validate(&allowed).is_valid());
}

#[test]
fn every_endpoint_alias_is_validated_without_disclosing_queries() {
    let resolved = make_resolved(
        StorageProvider::AwsS3,
        &[
            ("aws_endpoint", "https://valid.invalid"),
            (
                "aws_endpoint_url_s3",
                "https://signed.invalid?signature=do-not-print",
            ),
        ],
    );
    let result = CloudConfigValidator::validate(&resolved);
    assert!(!result.is_valid());
    let message = result.error_message();
    assert!(message.contains("aws_endpoint_url_s3"));
    assert!(!message.contains("do-not-print"));
    assert!(!message.contains("signed.invalid"));
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
    let resolved = make_resolved(
        StorageProvider::AwsS3,
        &[("aws_web_identity_token_file", "/var/run/token")],
    );
    let result = CloudConfigValidator::validate(&resolved);
    assert!(!result.is_valid());
    let msg = result.error_message();
    assert!(msg.contains("aws_role_arn"));
}

#[test]
fn test_error_includes_env_var_hint() {
    let resolved = make_resolved(
        StorageProvider::AwsS3,
        &[("aws_web_identity_token_file", "/var/run/token")],
    );
    let result = CloudConfigValidator::validate(&resolved);
    let role_err = result
        .errors
        .iter()
        .find(|e| e.key == "aws_role_arn")
        .unwrap();
    assert_eq!(role_err.env_var.as_deref(), Some("AWS_ROLE_ARN"));
    assert!(role_err.message.contains("aws_role_arn"));
}
