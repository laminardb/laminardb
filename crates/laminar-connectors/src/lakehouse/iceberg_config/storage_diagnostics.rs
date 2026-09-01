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
        if self.endpoint.is_some() {
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
        let has_key = |needles: &[&str]| {
            self.properties.keys().any(|key| {
                let key = key.to_ascii_lowercase();
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
        ]) {
            return AuthSource::ExplicitStatic;
        }
        if has_key(&["profile"]) {
            return AuthSource::Profile;
        }
        classify_storage_auth_source(
            self.diagnostic_provider(warehouse),
            &HashMap::new(),
            &|name| std::env::var(name).ok(),
        )
    }
}
