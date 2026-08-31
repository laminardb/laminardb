use std::collections::HashMap;
use std::fmt::Write;

use sha2::{Digest, Sha256};

use super::{IcebergCatalogConfig, IcebergStorageConfig};

pub(crate) fn stable_catalog_identity(
    catalog: &IcebergCatalogConfig,
    storage: &IcebergStorageConfig,
) -> String {
    let mut hasher = Sha256::new();
    hasher.update(b"laminardb-iceberg-catalog-identity-v2\0");
    for (key, value) in [
        ("catalog.type", catalog.catalog_type.to_string()),
        ("catalog.uri", catalog.catalog_uri.clone()),
        ("catalog.warehouse", catalog.warehouse.clone()),
        ("catalog.prefix", catalog.prefix.clone().unwrap_or_default()),
        ("catalog.auth.type", catalog.auth_type.to_string()),
        (
            "catalog.oauth2.server_uri",
            catalog.oauth2_server_uri.clone().unwrap_or_default(),
        ),
        (
            "catalog.oauth2.client_id",
            catalog.oauth2_client_id.clone().unwrap_or_default(),
        ),
        (
            "catalog.oauth2.scope",
            catalog.oauth2_scope.clone().unwrap_or_default(),
        ),
        (
            "catalog.access_delegation",
            catalog.access_delegation.to_string(),
        ),
        (
            "storage.type",
            storage
                .storage_type
                .map(|storage| storage.to_string())
                .unwrap_or_default(),
        ),
        (
            "storage.endpoint",
            storage.endpoint.clone().unwrap_or_default(),
        ),
        ("storage.region", storage.region.clone().unwrap_or_default()),
        ("storage.path_style", storage.path_style.to_string()),
        ("storage.encryption", storage.encryption.to_string()),
        (
            "storage.kms_key",
            storage.kms_key.clone().unwrap_or_default(),
        ),
    ] {
        hash_entry(&mut hasher, key, &value);
    }
    hash_properties(&mut hasher, "catalog.property", &catalog.properties);
    hash_properties(&mut hasher, "storage.property", &storage.properties);

    let mut encoded = String::with_capacity(64);
    for byte in hasher.finalize() {
        let _ = write!(encoded, "{byte:02x}");
    }
    encoded
}

fn hash_properties(hasher: &mut Sha256, namespace: &str, properties: &HashMap<String, String>) {
    let mut properties = properties.iter().collect::<Vec<_>>();
    properties.sort_unstable_by(|left, right| left.0.cmp(right.0));
    for (key, value) in properties {
        let key = format!("{namespace}.{key}");
        hash_entry(hasher, &key, value);
    }
}

fn hash_entry(hasher: &mut Sha256, key: &str, value: &str) {
    let value = crate::security::sanitize_identity_value(key, value);
    for component in [key, value.as_str()] {
        hasher.update(
            u64::try_from(component.len())
                .unwrap_or(u64::MAX)
                .to_le_bytes(),
        );
        hasher.update(component.as_bytes());
    }
}
