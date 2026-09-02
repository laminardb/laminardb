//! Non-secret storage classifications used by Iceberg tracing.

use std::collections::HashMap;

use laminar_core::storage_auth::{classify_storage_auth_source, AuthSource};

use crate::storage::{StorageEndpointClass, StorageLocation, StorageProvider};

use super::{IcebergStorageConfig, IcebergStorageType};

impl IcebergStorageConfig {
    pub(crate) fn diagnostic_provider(&self, warehouse: &str) -> StorageProvider {
        self.storage_type.map_or_else(
            || StorageProvider::detect(warehouse),
            |storage_type| match storage_type {
                IcebergStorageType::S3 => StorageProvider::AwsS3,
                IcebergStorageType::Gcs => StorageProvider::Gcs,
                IcebergStorageType::Azure => StorageProvider::AzureAdls,
                IcebergStorageType::Fs => StorageProvider::Local,
            },
        )
    }

    pub(crate) fn diagnostic_endpoint_class(&self, warehouse: &str) -> StorageEndpointClass {
        let provider = self.diagnostic_provider(warehouse);
        let property_endpoint = self.properties.iter().any(|(key, value)| {
            !value.trim().is_empty()
                && match provider {
                    StorageProvider::AwsS3 => key.eq_ignore_ascii_case("s3.endpoint"),
                    StorageProvider::Gcs => key.eq_ignore_ascii_case("gcs.service.path"),
                    StorageProvider::AzureAdls | StorageProvider::Local => false,
                }
        });
        if self
            .endpoint
            .as_ref()
            .is_some_and(|value| !value.trim().is_empty())
            || property_endpoint
        {
            return match provider {
                StorageProvider::Local => StorageEndpointClass::Local,
                StorageProvider::AwsS3 => StorageEndpointClass::S3Compatible,
                StorageProvider::AzureAdls | StorageProvider::Gcs => {
                    StorageEndpointClass::CustomOrEmulator
                }
            };
        }
        StorageLocation::parse(warehouse).map_or_else(
            |_| match provider {
                StorageProvider::Local => StorageEndpointClass::Local,
                StorageProvider::AwsS3 | StorageProvider::AzureAdls | StorageProvider::Gcs => {
                    StorageEndpointClass::Native
                }
            },
            |location| location.endpoint_class(),
        )
    }

    pub(crate) fn diagnostic_auth_source(&self, warehouse: &str) -> AuthSource {
        let provider = self.diagnostic_provider(warehouse);
        let has_key = |needles: &[&str]| {
            self.properties.iter().any(|(key, value)| {
                if value.trim().is_empty()
                    || property_provider(key).is_some_and(|owner| owner != provider)
                {
                    return false;
                }
                let key = key.to_ascii_lowercase().replace(['_', '.'], "-");
                needles.iter().any(|needle| key.contains(needle))
            })
        };
        if has_key(&["web-identity"]) {
            return AuthSource::WebIdentity;
        }
        if has_key(&["workload-identity", "federated-token"]) {
            return AuthSource::WorkloadIdentity;
        }
        if has_key(&["token", "sas-"]) {
            return AuthSource::ExplicitToken;
        }
        if has_key(&[
            "secret",
            "access-key",
            "account-key",
            "credential",
            "client-secret",
            "service-account",
        ]) {
            return AuthSource::ExplicitStatic;
        }
        if has_key(&["profile"]) {
            return AuthSource::Profile;
        }
        classify_storage_auth_source(provider, &HashMap::new(), &|name| std::env::var(name).ok())
    }
}

fn property_provider(key: &str) -> Option<StorageProvider> {
    let normalized = key.to_ascii_lowercase().replace(['_', '.'], "-");
    if normalized.starts_with("s3-") || normalized.starts_with("aws-") {
        return Some(StorageProvider::AwsS3);
    }
    if normalized.starts_with("gcs-")
        || normalized.starts_with("gcp-")
        || normalized.starts_with("google-")
    {
        return Some(StorageProvider::Gcs);
    }
    if normalized.starts_with("adls-")
        || normalized.starts_with("azdls-")
        || normalized.starts_with("azure-")
    {
        return Some(StorageProvider::AzureAdls);
    }
    None
}

#[cfg(test)]
mod tests {
    use crate::config::ConnectorConfig;

    use super::*;

    fn storage(entries: &[(&str, &str)]) -> IcebergStorageConfig {
        let mut config = ConnectorConfig::new("iceberg");
        for (key, value) in entries {
            config.set(*key, *value);
        }
        IcebergStorageConfig::from_config(&config).unwrap()
    }

    #[test]
    fn provider_property_endpoints_are_classified_as_compatibility() {
        for (storage_type, property, warehouse) in [
            ("s3", "storage.property.s3.endpoint", "s3://bucket/path"),
            (
                "gcs",
                "storage.property.gcs.service.path",
                "gs://bucket/path",
            ),
        ] {
            let config = storage(&[("storage.type", storage_type), (property, "http://test")]);
            assert_ne!(
                config.diagnostic_endpoint_class(warehouse),
                StorageEndpointClass::Native
            );
        }
    }

    #[test]
    fn auth_diagnostics_normalize_property_keys_and_ignore_empty_values() {
        let web_identity = storage(&[
            ("storage.type", "s3"),
            (
                "storage.property.aws_web_identity_token_file",
                "/var/run/token",
            ),
        ]);
        assert_eq!(
            web_identity.diagnostic_auth_source("s3://bucket/path"),
            AuthSource::WebIdentity
        );

        let workload = storage(&[
            ("storage.type", "azure"),
            (
                "storage.property.azure_federated_token_file",
                "/var/run/token",
            ),
        ]);
        assert_eq!(
            workload.diagnostic_auth_source("abfss://filesystem@account.dfs.core.windows.net/path"),
            AuthSource::WorkloadIdentity
        );

        let empty_token = storage(&[
            ("storage.type", "s3"),
            ("storage.property.session_token", ""),
            ("storage.property.secret_access_key", "configured"),
        ]);
        assert_eq!(
            empty_token.diagnostic_auth_source("s3://bucket/path"),
            AuthSource::ExplicitStatic
        );
    }

    #[test]
    fn diagnostics_ignore_properties_owned_by_another_provider() {
        let config = storage(&[
            ("storage.type", "s3"),
            ("storage.property.gcs.service.path", "http://gcs-emulator"),
            ("storage.property.gcs.oauth2.token", "gcs-token"),
        ]);
        assert_eq!(
            config.diagnostic_endpoint_class("s3://bucket/path"),
            StorageEndpointClass::Native
        );
        assert_ne!(
            config.diagnostic_auth_source("s3://bucket/path"),
            AuthSource::ExplicitToken
        );
    }
}
