//! Non-secret classification of object-store credential discovery.
//!
//! Provider clients retain ownership of credential loading and refresh. This
//! module inspects only whether configured sources are present so callers can
//! report useful diagnostics without retaining or displaying secret values.

#![allow(clippy::disallowed_types)] // cold path: storage configuration

use std::collections::HashMap;
use std::fmt;
use std::hash::BuildHasher;

use crate::storage_location::StorageProvider;

/// Non-secret description of the selected credential mechanism.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AuthSource {
    /// Long-lived credentials were supplied in connector configuration.
    ExplicitStatic,
    /// A bearer or session token was supplied in connector configuration.
    ExplicitToken,
    /// Long-lived credentials were discovered in provider environment variables.
    EnvironmentStatic,
    /// A bearer, session, or shared-access token was discovered in the environment.
    EnvironmentToken,
    /// An explicitly selected provider profile will be loaded downstream.
    Profile,
    /// AWS web identity will be exchanged by the downstream provider.
    WebIdentity,
    /// Federated workload identity will be loaded by the downstream provider.
    WorkloadIdentity,
    /// Instance, container, or managed-identity metadata will be used downstream.
    ManagedIdentityOrMetadata,
    /// Google Application Default Credentials will be loaded downstream.
    ApplicationDefault,
    /// An existing Azure CLI session will be loaded downstream.
    AzureCli,
    /// Requests are deliberately unsigned for a compatibility/test endpoint.
    Anonymous,
    /// The pinned provider library retains responsibility for credential discovery.
    DownstreamDefault,
    /// The credential mechanism could not be classified.
    Unknown,
}

impl fmt::Display for AuthSource {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::ExplicitStatic => "explicit-static",
            Self::ExplicitToken => "explicit-token",
            Self::EnvironmentStatic => "environment-static",
            Self::EnvironmentToken => "environment-token",
            Self::Profile => "profile",
            Self::WebIdentity => "web-identity",
            Self::WorkloadIdentity => "workload-identity",
            Self::ManagedIdentityOrMetadata => "managed-identity-or-metadata",
            Self::ApplicationDefault => "application-default",
            Self::AzureCli => "azure-cli",
            Self::Anonymous => "anonymous",
            Self::DownstreamDefault => "downstream-default",
            Self::Unknown => "unknown",
        })
    }
}

/// Classify a provider's effective credential source without loading credential values.
///
/// Explicit non-empty options take precedence over ambient sources. The environment lookup is
/// intentionally borrowed by callback so short-lived values remain owned by the downstream
/// provider's refreshable credential chain.
#[must_use]
pub fn classify_storage_auth_source<F, S>(
    provider: StorageProvider,
    explicit: &HashMap<String, String, S>,
    env_lookup: &F,
) -> AuthSource
where
    F: Fn(&str) -> Option<String>,
    S: BuildHasher,
{
    let signals = AuthSignals {
        explicit,
        env_lookup,
    };
    match provider {
        StorageProvider::AwsS3 => classify_aws(&signals),
        StorageProvider::AzureAdls => classify_azure(&signals),
        StorageProvider::Gcs => classify_gcs(&signals),
        StorageProvider::Local => AuthSource::Anonymous,
    }
}

struct AuthSignals<'a, F, S> {
    explicit: &'a HashMap<String, String, S>,
    env_lookup: &'a F,
}

impl<F, S> AuthSignals<'_, F, S>
where
    F: Fn(&str) -> Option<String>,
    S: BuildHasher,
{
    fn explicit_has(&self, keys: &[&str]) -> bool {
        self.explicit.iter().any(|(candidate, value)| {
            keys.iter().any(|key| candidate.eq_ignore_ascii_case(key)) && !value.trim().is_empty()
        })
    }

    fn explicit_true(&self, keys: &[&str]) -> bool {
        self.explicit.iter().any(|(candidate, value)| {
            keys.iter().any(|key| candidate.eq_ignore_ascii_case(key))
                && value.trim().eq_ignore_ascii_case("true")
        })
    }

    fn env_has(&self, keys: &[&str]) -> bool {
        keys.iter()
            .any(|key| (self.env_lookup)(key).is_some_and(|value| !value.trim().is_empty()))
    }

    fn env_true(&self, keys: &[&str]) -> bool {
        keys.iter().any(|key| {
            (self.env_lookup)(key).is_some_and(|value| value.trim().eq_ignore_ascii_case("true"))
        })
    }
}

fn classify_aws<F, S>(signals: &AuthSignals<'_, F, S>) -> AuthSource
where
    F: Fn(&str) -> Option<String>,
    S: BuildHasher,
{
    if signals.explicit_true(&["aws_skip_signature", "skip_signature"]) {
        AuthSource::Anonymous
    } else if signals.explicit_has(&["aws_session_token", "aws_token", "session_token", "token"]) {
        AuthSource::ExplicitToken
    } else if signals.explicit_has(&["aws_access_key_id", "access_key_id"])
        || signals.explicit_has(&["aws_secret_access_key", "secret_access_key"])
    {
        AuthSource::ExplicitStatic
    } else if signals.explicit_has(&["aws_profile", "profile"]) {
        AuthSource::Profile
    } else if signals.explicit_has(&["aws_web_identity_token_file", "web_identity_token_file"]) {
        AuthSource::WebIdentity
    } else if signals.env_has(&["AWS_SESSION_TOKEN"]) {
        AuthSource::EnvironmentToken
    } else if signals.env_has(&["AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY"]) {
        AuthSource::EnvironmentStatic
    } else if signals.env_has(&["AWS_WEB_IDENTITY_TOKEN_FILE"]) {
        AuthSource::WebIdentity
    } else if signals.env_has(&["AWS_PROFILE"]) {
        AuthSource::Profile
    } else if signals.env_has(&[
        "AWS_CONTAINER_CREDENTIALS_RELATIVE_URI",
        "AWS_CONTAINER_CREDENTIALS_FULL_URI",
    ]) {
        AuthSource::ManagedIdentityOrMetadata
    } else {
        AuthSource::DownstreamDefault
    }
}

fn classify_azure<F, S>(signals: &AuthSignals<'_, F, S>) -> AuthSource
where
    F: Fn(&str) -> Option<String>,
    S: BuildHasher,
{
    if signals.explicit_true(&["azure_skip_signature", "skip_signature"])
        || signals.explicit_true(&["azure_storage_use_emulator", "use_emulator"])
    {
        AuthSource::Anonymous
    } else if signals.explicit_has(&[
        "azure_storage_account_key",
        "azure_storage_access_key",
        "account_key",
        "azure_storage_client_secret",
        "azure_client_secret",
        "client_secret",
    ]) {
        AuthSource::ExplicitStatic
    } else if signals.explicit_has(&[
        "azure_storage_sas_token",
        "azure_storage_sas_key",
        "sas_token",
        "azure_storage_token",
        "bearer_token",
    ]) {
        AuthSource::ExplicitToken
    } else if signals.explicit_has(&["azure_federated_token_file", "federated_token_file"]) {
        AuthSource::WorkloadIdentity
    } else if signals.explicit_true(&["azure_use_azure_cli", "use_azure_cli"]) {
        AuthSource::AzureCli
    } else if signals.env_has(&["AZURE_STORAGE_SAS_TOKEN", "AZURE_STORAGE_TOKEN"]) {
        AuthSource::EnvironmentToken
    } else if signals.env_has(&["AZURE_STORAGE_ACCOUNT_KEY", "AZURE_CLIENT_SECRET"]) {
        AuthSource::EnvironmentStatic
    } else if signals.env_has(&["AZURE_FEDERATED_TOKEN_FILE"]) {
        AuthSource::WorkloadIdentity
    } else if signals.env_true(&["AZURE_USE_AZURE_CLI"]) {
        AuthSource::AzureCli
    } else {
        AuthSource::ManagedIdentityOrMetadata
    }
}

fn classify_gcs<F, S>(signals: &AuthSignals<'_, F, S>) -> AuthSource
where
    F: Fn(&str) -> Option<String>,
    S: BuildHasher,
{
    if signals.explicit_true(&["google_skip_signature", "skip_signature"])
        || signals.explicit_true(&["gcs.no-auth"])
    {
        AuthSource::Anonymous
    } else if signals.explicit_has(&[
        "google_service_account_key",
        "service_account_key",
        "google_service_account_path",
        "service_account_path",
    ]) {
        AuthSource::ExplicitStatic
    } else if signals.explicit_has(&["gcs.token", "google_token"]) {
        AuthSource::ExplicitToken
    } else if signals.explicit_has(&["google_application_credentials", "application_credentials"])
        || signals.env_has(&["GOOGLE_APPLICATION_CREDENTIALS"])
    {
        AuthSource::ApplicationDefault
    } else if signals.env_has(&[
        "GOOGLE_SERVICE_ACCOUNT_KEY",
        "SERVICE_ACCOUNT",
        "GOOGLE_SERVICE_ACCOUNT",
        "GOOGLE_SERVICE_ACCOUNT_PATH",
    ]) {
        AuthSource::EnvironmentStatic
    } else {
        AuthSource::ApplicationDefault
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn lookup(values: &[(&str, &str)], key: &str) -> Option<String> {
        values
            .iter()
            .find_map(|(candidate, value)| (*candidate == key).then(|| (*value).to_string()))
    }

    #[test]
    fn classifies_refreshable_ambient_sources_without_values() {
        let options = HashMap::new();
        assert_eq!(
            classify_storage_auth_source(StorageProvider::AzureAdls, &options, &|key| lookup(
                &[("AZURE_FEDERATED_TOKEN_FILE", "/token")],
                key
            )),
            AuthSource::WorkloadIdentity
        );
        assert_eq!(
            classify_storage_auth_source(StorageProvider::AwsS3, &options, &|key| lookup(
                &[("AWS_CONTAINER_CREDENTIALS_RELATIVE_URI", "/task")],
                key
            )),
            AuthSource::ManagedIdentityOrMetadata
        );
        assert_eq!(
            classify_storage_auth_source(StorageProvider::Gcs, &options, &|_| None),
            AuthSource::ApplicationDefault
        );
    }

    #[test]
    fn display_is_low_cardinality_and_non_secret() {
        assert_eq!(AuthSource::WebIdentity.to_string(), "web-identity");
        assert_eq!(AuthSource::AzureCli.to_string(), "azure-cli");
    }

    #[test]
    fn explicit_azure_credentials_take_precedence_over_ambient_cli() {
        let options = HashMap::from([(
            "azure_storage_account_key".to_string(),
            "not-logged".to_string(),
        )]);
        assert_eq!(
            classify_storage_auth_source(StorageProvider::AzureAdls, &options, &|key| lookup(
                &[("AZURE_USE_AZURE_CLI", "true")],
                key
            )),
            AuthSource::ExplicitStatic
        );
    }

    #[test]
    fn explicit_gcs_token_has_a_non_secret_source_classification() {
        let options = HashMap::from([("gcs.token".to_string(), "not-logged".to_string())]);
        assert_eq!(
            classify_storage_auth_source(StorageProvider::Gcs, &options, &|_| None),
            AuthSource::ExplicitToken
        );
    }
}
