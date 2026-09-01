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
