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

    let resolved = StorageCredentialResolver::resolve_with_env("s3://bucket/path", &opts, aws_env);
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

    let resolved = StorageCredentialResolver::resolve_with_env("s3://bucket/path", &opts, aws_env);
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
    let resolved =
        StorageCredentialResolver::resolve_with_env("s3://bucket/path", &empty_opts(), env_none);
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

    let resolved = StorageCredentialResolver::resolve_with_env("s3://bucket/path", &opts, env_none);
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

    let resolved = StorageCredentialResolver::resolve_with_env("s3://bucket/path", &opts, env_none);
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
    let resolved =
        StorageCredentialResolver::resolve_with_env("az://container/path", &empty_opts(), env_none);
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

    let resolved = StorageCredentialResolver::resolve_with_env("gs://bucket/path", &opts, env_none);
    assert!(resolved.has_credentials());
}

#[test]
fn test_resolve_gcs_no_credentials() {
    let resolved =
        StorageCredentialResolver::resolve_with_env("gs://bucket/path", &empty_opts(), env_none);
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
    let resolved =
        StorageCredentialResolver::resolve_with_env("s3a://bucket/path", &empty_opts(), aws_env);
    assert_eq!(resolved.provider, StorageProvider::AwsS3);
    assert!(resolved.has_credentials());
}
