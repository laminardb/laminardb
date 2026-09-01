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
    assert!(resolved.options.is_empty());
    assert_eq!(resolved.env_resolved_keys.len(), 3);
    assert_eq!(resolved.auth_source, AuthSource::EnvironmentStatic);
    assert!(resolved.has_credentials());
}

#[test]
fn test_resolve_s3_explicit_overrides_env() {
    let mut opts = HashMap::new();
    opts.insert("aws_region".to_string(), "ap-southeast-1".to_string());

    let resolved = StorageCredentialResolver::resolve_with_env("s3://bucket/path", &opts, aws_env);
    // Explicit region is retained; ambient credentials are observed but not copied.
    assert_eq!(resolved.options["aws_region"], "ap-southeast-1");
    assert!(!resolved.options.contains_key("aws_access_key_id"));
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
    assert!(!resolved.options.contains_key("aws_session_token"));
    assert_eq!(resolved.auth_source, AuthSource::EnvironmentToken);
    assert!(resolved
        .env_resolved_keys
        .contains(&"aws_session_token".to_string()));
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
    assert_eq!(
        resolved.endpoint_class(),
        laminar_core::storage_location::StorageEndpointClass::S3Compatible
    );
}

#[test]
fn endpoint_class_is_low_cardinality_and_provider_specific() {
    let local = StorageCredentialResolver::resolve_with_env("/data/table", &empty_opts(), env_none);
    assert_eq!(local.endpoint_class().to_string(), "local");

    let gcs = StorageCredentialResolver::resolve_with_env(
        "gs://bucket/path",
        &HashMap::from([("google_base_url".into(), "http://emulator.invalid".into())]),
        env_none,
    );
    assert_eq!(gcs.endpoint_class().to_string(), "custom-or-emulator");

    let azure =
        StorageCredentialResolver::resolve_with_env("az://container/path", &empty_opts(), env_none);
    assert_eq!(azure.endpoint_class().to_string(), "native");
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
    assert!(resolved.options.is_empty());
    assert!(resolved
        .env_resolved_keys
        .contains(&"azure_storage_account_name".to_string()));
    assert!(resolved
        .env_resolved_keys
        .contains(&"azure_storage_account_key".to_string()));
    assert_eq!(resolved.auth_source, AuthSource::EnvironmentStatic);
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
    assert_eq!(resolved.auth_source, AuthSource::ManagedIdentityOrMetadata);
}

#[test]
fn test_resolve_azure_no_credentials() {
    let resolved =
        StorageCredentialResolver::resolve_with_env("az://container/path", &empty_opts(), env_none);
    assert!(!resolved.has_credentials());
}

#[test]
fn azure_cli_selection_is_classified_without_copying_environment_state() {
    let environment = |name: &str| (name == "AZURE_USE_AZURE_CLI").then(|| "true".to_string());
    let resolved = StorageCredentialResolver::resolve_with_env(
        "az://container/path",
        &empty_opts(),
        environment,
    );
    assert_eq!(resolved.auth_source, AuthSource::AzureCli);
    assert!(resolved.has_credentials());
    assert!(!resolved.options.contains_key("azure_use_azure_cli"));
}

// ── GCS tests ──

#[test]
fn test_resolve_gcs_env_fallback() {
    let resolved =
        StorageCredentialResolver::resolve_with_env("gs://bucket/path", &empty_opts(), gcs_env);
    assert_eq!(resolved.provider, StorageProvider::Gcs);
    assert!(!resolved
        .options
        .contains_key("google_application_credentials"));
    assert!(resolved
        .env_resolved_keys
        .contains(&"google_application_credentials".to_string()));
    assert_eq!(resolved.auth_source, AuthSource::ApplicationDefault);
}

#[test]
fn test_resolve_gcs_service_account_path_aliases() {
    for variable in [
        "SERVICE_ACCOUNT",
        "GOOGLE_SERVICE_ACCOUNT",
        "GOOGLE_SERVICE_ACCOUNT_PATH",
    ] {
        let environment = |candidate: &str| {
            (candidate == variable).then(|| "/path/to/service-account.json".to_string())
        };
        let resolved = StorageCredentialResolver::resolve_with_env(
            "gs://bucket/path",
            &empty_opts(),
            environment,
        );
        assert_eq!(resolved.auth_source, AuthSource::EnvironmentStatic);
        assert!(resolved
            .env_resolved_keys
            .contains(&"google_service_account_path".to_string()));
        assert!(!resolved.options.contains_key("google_service_account_path"));
    }
}

#[test]
fn unsupported_gcs_access_token_is_not_reported_as_selected_authentication() {
    let options = HashMap::from([("gcs.token".to_string(), "short-lived-token".to_string())]);
    let resolved =
        StorageCredentialResolver::resolve_with_env("gs://bucket/path", &options, env_none);
    assert_eq!(resolved.auth_source, AuthSource::Unknown);
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

#[test]
fn short_lived_and_ambient_sources_are_classified_without_loading_tokens() {
    let web_identity = |name: &str| match name {
        "AWS_WEB_IDENTITY_TOKEN_FILE" => Some("/var/run/secrets/token".into()),
        "AWS_ROLE_ARN" => Some("role-reference".into()),
        _ => None,
    };
    let aws = StorageCredentialResolver::resolve_with_env(
        "s3://bucket/path",
        &empty_opts(),
        web_identity,
    );
    assert_eq!(aws.auth_source, AuthSource::WebIdentity);
    assert!(!aws.options.contains_key("aws_web_identity_token_file"));
    assert!(aws
        .env_resolved_keys
        .contains(&"aws_web_identity_token_file".to_string()));

    let container = |name: &str| match name {
        "AWS_CONTAINER_CREDENTIALS_RELATIVE_URI" => Some("/v2/credentials/task".into()),
        _ => None,
    };
    let aws =
        StorageCredentialResolver::resolve_with_env("s3://bucket/path", &empty_opts(), container);
    assert_eq!(aws.auth_source, AuthSource::ManagedIdentityOrMetadata);

    let azure = |name: &str| match name {
        "AZURE_FEDERATED_TOKEN_FILE" => Some("/var/run/secrets/azure-token".into()),
        "AZURE_CLIENT_ID" => Some("client-reference".into()),
        "AZURE_TENANT_ID" => Some("tenant-reference".into()),
        _ => None,
    };
    let azure =
        StorageCredentialResolver::resolve_with_env("az://container/path", &empty_opts(), azure);
    assert_eq!(azure.auth_source, AuthSource::WorkloadIdentity);
    assert!(!azure.options.contains_key("azure_federated_token_file"));
}

#[test]
fn ambient_endpoint_is_classified_without_retaining_its_value() {
    let environment = |name: &str| match name {
        "AWS_ENDPOINT_URL" => Some("http://secret-bearing-host.invalid".into()),
        _ => None,
    };
    let resolved =
        StorageCredentialResolver::resolve_with_env("s3://bucket/path", &empty_opts(), environment);
    assert!(resolved.options.is_empty());
    assert_eq!(
        resolved.endpoint_class(),
        laminar_core::storage_location::StorageEndpointClass::S3Compatible
    );
    assert!(!format!("{resolved:?}").contains("secret-bearing-host"));
}

#[test]
fn qualified_azure_authority_supplies_non_secret_client_options() {
    let resolved = StorageCredentialResolver::resolve_with_env(
        "wasbs://container@account.blob.core.usgovcloudapi.net/path",
        &empty_opts(),
        env_none,
    );
    assert_eq!(resolved.options["azure_storage_account_name"], "account");
    assert_eq!(resolved.options["azure_container_name"], "container");
    assert_eq!(
        resolved.options["azure_endpoint"],
        "https://account.blob.core.usgovcloudapi.net"
    );
    assert_eq!(resolved.endpoint_class(), StorageEndpointClass::Native);
    assert!(!resolved.endpoint_override_configured);

    let private = StorageCredentialResolver::resolve_with_env(
        "abfss://filesystem@account.dfs.storage.private.example/path",
        &empty_opts(),
        env_none,
    );
    assert_eq!(
        private.endpoint_class(),
        StorageEndpointClass::CustomOrEmulator
    );
    assert!(!private.endpoint_override_configured);
}

#[test]
fn false_ambient_allow_http_is_not_an_effective_override() {
    let environment = |name: &str| (name == "AWS_ALLOW_HTTP").then(|| "false".to_string());
    let resolved =
        StorageCredentialResolver::resolve_with_env("s3://bucket/path", &empty_opts(), environment);
    assert!(!resolved
        .env_resolved_keys
        .contains(&"aws_allow_http".to_string()));
}

#[test]
fn resolved_debug_contains_only_option_keys_and_auth_category() {
    let options = HashMap::from([
        ("aws_access_key_id".into(), "access-id".into()),
        ("aws_secret_access_key".into(), "secret-value".into()),
    ]);
    let resolved =
        StorageCredentialResolver::resolve_with_env("s3://bucket/path", &options, env_none);
    let debug = format!("{resolved:?}");
    assert!(debug.contains("ExplicitStatic"));
    assert!(debug.contains("aws_access_key_id"));
    assert!(!debug.contains("access-id"));
    assert!(!debug.contains("secret-value"));
}
