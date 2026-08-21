use super::*;

// ── is_secret_key tests ──

#[test]
fn test_secret_key_aws_secret() {
    assert!(SecretMasker::is_secret_key("aws_secret_access_key"));
}

#[test]
fn test_secret_key_password() {
    assert!(SecretMasker::is_secret_key("password"));
}

#[test]
fn test_secret_key_azure_account_key() {
    assert!(SecretMasker::is_secret_key("azure_storage_account_key"));
}

#[test]
fn test_secret_key_sas_token() {
    assert!(SecretMasker::is_secret_key("azure_storage_sas_token"));
}

#[test]
fn test_secret_key_session_token() {
    assert!(SecretMasker::is_secret_key("aws_session_token"));
}

#[test]
fn test_secret_key_private_key() {
    assert!(SecretMasker::is_secret_key("google_private_key"));
}

#[test]
fn test_secret_key_service_account_key() {
    assert!(SecretMasker::is_secret_key("google_service_account_key"));
}

#[test]
fn test_secret_key_client_secret() {
    assert!(SecretMasker::is_secret_key("azure_storage_client_secret"));
}

#[test]
fn test_secret_key_case_insensitive() {
    assert!(SecretMasker::is_secret_key("AWS_SECRET_ACCESS_KEY"));
    assert!(SecretMasker::is_secret_key("Password"));
}

#[test]
fn test_not_secret_region() {
    assert!(!SecretMasker::is_secret_key("aws_region"));
}

#[test]
fn test_not_secret_access_key_id() {
    assert!(!SecretMasker::is_secret_key("aws_access_key_id"));
}

#[test]
fn test_not_secret_table_path() {
    assert!(!SecretMasker::is_secret_key("table.path"));
}

#[test]
fn test_not_secret_account_name() {
    assert!(!SecretMasker::is_secret_key("azure_storage_account_name"));
}

#[test]
fn test_not_secret_endpoint() {
    assert!(!SecretMasker::is_secret_key("aws_endpoint"));
}

#[test]
fn test_not_secret_service_account_path() {
    // Path is not secret (it's just a filesystem path to the key file)
    assert!(!SecretMasker::is_secret_key("google_service_account_path"));
}

// ── redact_map tests ──

#[test]
fn test_redact_map_replaces_secrets() {
    let mut map = HashMap::new();
    map.insert("aws_region".to_string(), "us-east-1".to_string());
    map.insert(
        "aws_secret_access_key".to_string(),
        "REAL_SECRET".to_string(),
    );
    map.insert("aws_access_key_id".to_string(), "AKID123".to_string());

    let redacted = SecretMasker::redact_map(&map);
    assert_eq!(redacted["aws_region"], "us-east-1");
    assert_eq!(redacted["aws_secret_access_key"], "***");
    assert_eq!(redacted["aws_access_key_id"], "AKID123");
}

#[test]
fn test_redact_map_empty() {
    let map = HashMap::new();
    let redacted = SecretMasker::redact_map(&map);
    assert!(redacted.is_empty());
}

// ── display_map tests ──

#[test]
fn test_display_map_sorted() {
    let mut map = HashMap::new();
    map.insert("z_key".to_string(), "z_val".to_string());
    map.insert("a_key".to_string(), "a_val".to_string());

    let display = SecretMasker::display_map(&map);
    assert!(display.starts_with("a_key="));
    assert!(display.contains("z_key="));
}

#[test]
fn test_display_map_redacts_secrets() {
    let mut map = HashMap::new();
    map.insert("aws_region".to_string(), "us-east-1".to_string());
    map.insert(
        "aws_secret_access_key".to_string(),
        "TOP_SECRET".to_string(),
    );

    let display = SecretMasker::display_map(&map);
    assert!(display.contains("aws_region=us-east-1"));
    assert!(display.contains("aws_secret_access_key=***"));
    assert!(!display.contains("TOP_SECRET"));
}

#[test]
fn test_display_map_empty() {
    let map = HashMap::new();
    let display = SecretMasker::display_map(&map);
    assert!(display.is_empty());
}
