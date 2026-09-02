//! Refreshable GCS workload-identity credentials.
//!
//! `object_store` 0.13 supports injecting a credential provider but cannot
//! parse Google external-account ADC files. This module bridges Google's
//! official refreshable provider into that existing hook without retaining or
//! logging access tokens in LaminarDB.

#![allow(clippy::disallowed_types)] // cold path: storage configuration

use std::collections::HashMap;
use std::fmt;
use std::hash::BuildHasher;
use std::io::Read;
use std::sync::Arc;

use google_cloud_auth::credentials::AccessTokenCredentials;
use object_store::client::CredentialProvider;
use object_store::gcp::{
    GcpCredential, GcpCredentialProvider, GoogleCloudStorageBuilder, GoogleConfigKey,
};
use parking_lot::Mutex;
use serde_json::Value;

const MAX_APPLICATION_CREDENTIAL_BYTES: usize = 1024 * 1024;
const GCS_READ_WRITE_SCOPE: &str = "https://www.googleapis.com/auth/devstorage.read_write";
const APPLICATION_CREDENTIAL_KEYS: &[&str] =
    &["google_application_credentials", "application_credentials"];
const EXPLICIT_SERVICE_ACCOUNT_KEYS: &[&str] = &[
    "google_service_account",
    "service_account",
    "google_service_account_path",
    "service_account_path",
    "google_service_account_key",
    "service_account_key",
];
const AMBIENT_SERVICE_ACCOUNT_KEYS: &[&str] = &[
    "SERVICE_ACCOUNT",
    "GOOGLE_SERVICE_ACCOUNT",
    "GOOGLE_SERVICE_ACCOUNT_PATH",
    "GOOGLE_SERVICE_ACCOUNT_KEY",
];

/// A redacted failure while selecting Google application credentials.
#[derive(Debug, thiserror::Error)]
pub enum GcsCredentialError {
    /// The selected credential file could not be read.
    #[error("the selected GCS application credentials file could not be read")]
    UnreadableApplicationCredentials,
    /// The selected credential file exceeds the bounded configuration size.
    #[error("the selected GCS application credentials file exceeds the 1 MiB limit")]
    ApplicationCredentialsTooLarge,
    /// The selected credential file is not valid JSON.
    #[error("the selected GCS application credentials file is not valid JSON")]
    InvalidApplicationCredentialsJson,
    /// A required external-account field was missing or had the wrong type.
    #[error("the selected GCS external-account credentials have an invalid '{0}' field")]
    InvalidExternalAccountField(&'static str),
    /// The selected ADC credential type is not supported by the pinned client.
    #[error("the selected GCS application credential type is unsupported")]
    UnsupportedApplicationCredentialType,
    /// Google's credential provider rejected the external-account configuration.
    #[error("the selected GCS external-account credentials are invalid")]
    InvalidExternalAccountConfiguration,
    /// Google's refreshable provider failed to acquire an access token.
    #[error("the GCS workload-identity provider could not acquire an access token")]
    TokenAcquisition,
    /// Google's refreshable provider returned an unusable access token.
    #[error("the GCS workload-identity provider returned an empty access token")]
    EmptyAccessToken,
}

/// Select a refreshable credential provider when external-account ADC wins the
/// existing explicit-over-ambient credential precedence.
///
/// Service-account key/path configuration remains owned by `object_store`.
/// The returned provider lazily initializes Google's SDK on first I/O so a
/// synchronously constructed object store is not tied to a temporary runtime.
///
/// # Errors
///
/// Returns a redacted error when the selected ADC file cannot be safely read,
/// parsed, or classified.
pub fn gcs_workload_identity_provider<F, S>(
    explicit: &HashMap<String, String, S>,
    env_lookup: &F,
) -> Result<Option<GcpCredentialProvider>, GcsCredentialError>
where
    F: Fn(&str) -> Option<String>,
    S: BuildHasher,
{
    if has_non_empty_option(explicit, EXPLICIT_SERVICE_ACCOUNT_KEYS) {
        return Ok(None);
    }
    if let Some(path) = non_empty_option(explicit, APPLICATION_CREDENTIAL_KEYS) {
        return load_workload_identity_provider(path);
    }
    if has_non_empty_environment(env_lookup, AMBIENT_SERVICE_ACCOUNT_KEYS) {
        return Ok(None);
    }
    let Some(path) =
        env_lookup("GOOGLE_APPLICATION_CREDENTIALS").filter(|value| !value.trim().is_empty())
    else {
        return Ok(None);
    };
    load_workload_identity_provider(&path)
}

/// Apply GCS options while keeping credential material owned by the injected
/// workload-identity provider.
///
/// Options are applied in iterator order, matching `object_store::parse_url_opts`.
/// Credential keys are deliberately omitted so ambient static credentials
/// cannot override an explicitly selected external-account source.
pub fn configure_gcs_workload_identity<I, K, V>(
    builder: GoogleCloudStorageBuilder,
    options: I,
    credentials: GcpCredentialProvider,
) -> GoogleCloudStorageBuilder
where
    I: IntoIterator<Item = (K, V)>,
    K: AsRef<str>,
    V: Into<String>,
{
    options
        .into_iter()
        .fold(builder, |builder, (key, value)| {
            let Ok(key) = key.as_ref().to_ascii_lowercase().parse::<GoogleConfigKey>() else {
                return builder;
            };
            if is_credential_key(key) {
                builder
            } else {
                builder.with_config(key, value)
            }
        })
        .with_credentials(credentials)
}

fn is_credential_key(key: GoogleConfigKey) -> bool {
    matches!(
        key,
        GoogleConfigKey::ServiceAccount
            | GoogleConfigKey::ServiceAccountKey
            | GoogleConfigKey::ApplicationCredentials
    )
}

fn non_empty_option<'a, S>(
    options: &'a HashMap<String, String, S>,
    keys: &[&str],
) -> Option<&'a str>
where
    S: BuildHasher,
{
    keys.iter().find_map(|key| {
        options.iter().find_map(|(candidate, value)| {
            (candidate.eq_ignore_ascii_case(key) && !value.trim().is_empty())
                .then_some(value.as_str())
        })
    })
}

fn has_non_empty_option<S>(options: &HashMap<String, String, S>, keys: &[&str]) -> bool
where
    S: BuildHasher,
{
    non_empty_option(options, keys).is_some()
}

fn has_non_empty_environment<F>(env_lookup: &F, keys: &[&str]) -> bool
where
    F: Fn(&str) -> Option<String>,
{
    keys.iter()
        .any(|key| env_lookup(key).is_some_and(|value| !value.trim().is_empty()))
}

fn load_workload_identity_provider(
    path: &str,
) -> Result<Option<GcpCredentialProvider>, GcsCredentialError> {
    let file = std::fs::File::open(path)
        .map_err(|_| GcsCredentialError::UnreadableApplicationCredentials)?;
    let mut bytes = Vec::new();
    file.take((MAX_APPLICATION_CREDENTIAL_BYTES + 1) as u64)
        .read_to_end(&mut bytes)
        .map_err(|_| GcsCredentialError::UnreadableApplicationCredentials)?;
    if bytes.len() > MAX_APPLICATION_CREDENTIAL_BYTES {
        return Err(GcsCredentialError::ApplicationCredentialsTooLarge);
    }
    let document: Value = serde_json::from_slice(&bytes)
        .map_err(|_| GcsCredentialError::InvalidApplicationCredentialsJson)?;
    let credential_type = required_string(&document, "type")?;
    match credential_type {
        "service_account" | "authorized_user" => Ok(None),
        "external_account" => {
            validate_external_account_document(&document)?;
            Ok(Some(Arc::new(WorkloadIdentityCredentialProvider {
                document: Mutex::new(Some(document)),
                credentials: tokio::sync::OnceCell::new(),
            })))
        }
        _ => Err(GcsCredentialError::UnsupportedApplicationCredentialType),
    }
}

fn validate_external_account_document(document: &Value) -> Result<(), GcsCredentialError> {
    for field in ["audience", "subject_token_type", "token_url"] {
        required_string(document, field)?;
    }
    if !document
        .get("credential_source")
        .is_some_and(Value::is_object)
    {
        return Err(GcsCredentialError::InvalidExternalAccountField(
            "credential_source",
        ));
    }
    Ok(())
}

fn required_string<'a>(
    document: &'a Value,
    field: &'static str,
) -> Result<&'a str, GcsCredentialError> {
    document
        .get(field)
        .and_then(Value::as_str)
        .filter(|value| !value.trim().is_empty())
        .ok_or(GcsCredentialError::InvalidExternalAccountField(field))
}

struct WorkloadIdentityCredentialProvider {
    document: Mutex<Option<Value>>,
    credentials: tokio::sync::OnceCell<AccessTokenCredentials>,
}

impl fmt::Debug for WorkloadIdentityCredentialProvider {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("GcsWorkloadIdentityCredentialProvider")
    }
}

#[async_trait::async_trait]
impl CredentialProvider for WorkloadIdentityCredentialProvider {
    type Credential = GcpCredential;

    async fn get_credential(&self) -> object_store::Result<Arc<GcpCredential>> {
        let credentials = self
            .credentials
            .get_or_try_init(|| async {
                let document = self.document.lock().take().ok_or_else(|| {
                    object_store_error(GcsCredentialError::InvalidExternalAccountConfiguration)
                })?;
                google_cloud_auth::credentials::external_account::Builder::new(document)
                    .with_scopes([GCS_READ_WRITE_SCOPE])
                    .build_access_token_credentials()
                    .map_err(|_| {
                        object_store_error(GcsCredentialError::InvalidExternalAccountConfiguration)
                    })
            })
            .await?;
        let token = credentials
            .access_token()
            .await
            .map_err(|_| object_store_error(GcsCredentialError::TokenAcquisition))?;
        if token.token.trim().is_empty() {
            return Err(object_store_error(GcsCredentialError::EmptyAccessToken));
        }
        Ok(Arc::new(GcpCredential {
            bearer: token.token,
        }))
    }
}

fn object_store_error(error: GcsCredentialError) -> object_store::Error {
    object_store::Error::Generic {
        store: "GCS authentication",
        source: Box::new(error),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn external_account_document(subject_token_file: &str) -> Value {
        serde_json::json!({
            "type": "external_account",
            "audience": "//iam.googleapis.com/projects/1/locations/global/workloadIdentityPools/test/providers/github",
            "subject_token_type": "urn:ietf:params:oauth:token-type:jwt",
            "token_url": "https://sts.googleapis.com/v1/token",
            "credential_source": { "file": subject_token_file }
        })
    }

    fn write_credentials(directory: &tempfile::TempDir, name: &str, document: &Value) -> String {
        let path = directory.path().join(name);
        std::fs::write(&path, serde_json::to_vec(document).unwrap()).unwrap();
        path.to_string_lossy().into_owned()
    }

    #[test]
    fn explicit_external_account_precedes_ambient_service_account() {
        let directory = tempfile::tempdir().unwrap();
        let path = write_credentials(
            &directory,
            "external.json",
            &external_account_document("subject-token"),
        );
        let options = HashMap::from([("google_application_credentials".into(), path)]);
        let provider = gcs_workload_identity_provider(&options, &|name| {
            (name == "GOOGLE_SERVICE_ACCOUNT_KEY").then(|| "not-selected".into())
        })
        .unwrap()
        .unwrap();
        assert_eq!(
            format!("{provider:?}"),
            "GcsWorkloadIdentityCredentialProvider"
        );
    }

    #[test]
    fn explicit_service_account_retains_downstream_provider() {
        let options =
            HashMap::from([("google_service_account_key".into(), "not-inspected".into())]);
        let selected = gcs_workload_identity_provider(&options, &|name| {
            (name == "GOOGLE_APPLICATION_CREDENTIALS").then(|| "ambient.json".into())
        })
        .unwrap();
        assert!(selected.is_none());
    }

    #[test]
    fn supported_non_external_adc_retains_downstream_provider() {
        let directory = tempfile::tempdir().unwrap();
        let path = write_credentials(
            &directory,
            "authorized-user.json",
            &serde_json::json!({ "type": "authorized_user" }),
        );
        let options = HashMap::from([("application_credentials".into(), path)]);
        assert!(gcs_workload_identity_provider(&options, &|_| None)
            .unwrap()
            .is_none());
    }

    #[test]
    fn errors_and_debug_output_do_not_disclose_credentials() {
        let directory = tempfile::tempdir().unwrap();
        let secret = "do-not-disclose";
        let path = write_credentials(
            &directory,
            secret,
            &serde_json::json!({
                "type": "external_account",
                "token_url": format!("https://{secret}.example/token"),
                "credential_source": { "file": secret }
            }),
        );
        let options = HashMap::from([("google_application_credentials".into(), path)]);
        let error = gcs_workload_identity_provider(&options, &|_| None)
            .unwrap_err()
            .to_string();
        assert!(error.contains("audience"), "{error}");
        assert!(!error.contains(secret), "{error}");
    }

    #[test]
    fn credential_file_read_is_bounded() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("oversized.json");
        std::fs::write(&path, vec![b'x'; MAX_APPLICATION_CREDENTIAL_BYTES + 1]).unwrap();
        let options = HashMap::from([(
            "google_application_credentials".into(),
            path.to_string_lossy().into_owned(),
        )]);

        let error = gcs_workload_identity_provider(&options, &|_| None).unwrap_err();
        assert!(matches!(
            error,
            GcsCredentialError::ApplicationCredentialsTooLarge
        ));
    }

    #[test]
    fn credential_options_are_not_applied_to_the_object_store_builder() {
        let directory = tempfile::tempdir().unwrap();
        let path = write_credentials(
            &directory,
            "external.json",
            &external_account_document("subject-token"),
        );
        let options = HashMap::from([("google_application_credentials".into(), path)]);
        let provider = gcs_workload_identity_provider(&options, &|_| None)
            .unwrap()
            .unwrap();
        let builder = configure_gcs_workload_identity(
            GoogleCloudStorageBuilder::new().with_url("gs://test-bucket/prefix"),
            options,
            provider,
        );
        assert!(builder.build().is_ok());
    }

    #[tokio::test]
    async fn external_account_provider_acquires_and_reuses_sdk_token() {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let token_url = format!("http://{}/token", listener.local_addr().unwrap());
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let mut request = [0_u8; 8 * 1024];
            let length = stream.read(&mut request).await.unwrap();
            assert!(String::from_utf8_lossy(&request[..length]).starts_with("POST /token "));
            let body = r#"{"access_token":"test-access-token","issued_token_type":"urn:ietf:params:oauth:token-type:access_token","token_type":"Bearer","expires_in":3600}"#;
            let response = format!(
                "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{body}",
                body.len()
            );
            stream.write_all(response.as_bytes()).await.unwrap();
        });

        let directory = tempfile::tempdir().unwrap();
        let subject_token_path = directory.path().join("subject-token");
        std::fs::write(&subject_token_path, "test-subject-token").unwrap();
        let credential_path = write_credentials(
            &directory,
            "external.json",
            &serde_json::json!({
                "type": "external_account",
                "audience": "test-audience",
                "subject_token_type": "urn:ietf:params:oauth:token-type:jwt",
                "token_url": token_url,
                "credential_source": { "file": subject_token_path }
            }),
        );
        let options = HashMap::from([("google_application_credentials".into(), credential_path)]);
        let provider = gcs_workload_identity_provider(&options, &|_| None)
            .unwrap()
            .unwrap();

        let first =
            tokio::time::timeout(std::time::Duration::from_secs(5), provider.get_credential())
                .await
                .unwrap()
                .unwrap();
        let second = provider.get_credential().await.unwrap();
        assert_eq!(first.bearer, "test-access-token");
        assert_eq!(second.bearer, first.bearer);
        server.await.unwrap();
    }
}
