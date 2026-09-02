//! Secret masking for safe logging of connector configuration.
//!
//! [`SecretMasker`] identifies keys that hold secret values (passwords,
//! access keys, tokens) and replaces their values with `"***"` in
//! `Display`/`Debug` output.
#![allow(clippy::disallowed_types)] // cold path: storage configuration

use std::collections::HashMap;

/// Substring patterns that indicate a key holds a secret value.
///
/// Matched case-insensitively against the full key name.
const SECRET_PATTERNS: &[&str] = &[
    "secret",
    "password",
    "access_key",
    "account_key",
    "private_key",
    "token",
    "credential",
    "service_account",
    "client_id",
    "tenant_id",
    "account_name",
    "profile",
    "role_arn",
    "authorization",
    "kms_key",
];

/// Utility for masking secret values in configuration maps.
pub struct SecretMasker;

impl SecretMasker {
    /// Returns true if the key name suggests it holds a secret value.
    ///
    /// Matches case-insensitively against known secret patterns.
    /// Credential identifiers are redacted as well as secret material so
    /// logs cannot be used to inventory cloud identities.
    ///
    /// # Examples
    ///
    /// ```
    /// use laminar_connectors::storage::SecretMasker;
    ///
    /// assert!(SecretMasker::is_secret_key("aws_secret_access_key"));
    /// assert!(SecretMasker::is_secret_key("password"));
    /// assert!(!SecretMasker::is_secret_key("aws_region"));
    /// assert!(SecretMasker::is_secret_key("aws_access_key_id"));
    /// ```
    #[must_use]
    pub fn is_secret_key(key: &str) -> bool {
        let lower = key.to_lowercase();
        SECRET_PATTERNS.iter().any(|p| lower.contains(p))
    }

    /// Returns a redacted copy of the map, replacing secret values with `"***"`.
    #[must_use]
    pub fn redact_map(map: &HashMap<String, String>) -> HashMap<String, String> {
        map.iter()
            .map(|(k, v)| (k.clone(), Self::redact_value(k, v)))
            .collect()
    }

    /// Formats a map for display with secrets redacted and keys sorted.
    #[must_use]
    pub fn display_map(map: &HashMap<String, String>) -> String {
        if map.is_empty() {
            return String::new();
        }

        let mut pairs: Vec<_> = map.iter().collect();
        pairs.sort_by_key(|(k, _)| k.as_str());
        pairs
            .iter()
            .map(|(k, v)| format!("{k}={}", Self::redact_value(k, v)))
            .collect::<Vec<_>>()
            .join(", ")
    }

    fn redact_value(key: &str, value: &str) -> String {
        if Self::is_secret_key(key) {
            return "***".into();
        }
        let normalized = key.to_ascii_lowercase();
        if normalized.contains("endpoint") || normalized.ends_with("base_url") {
            return endpoint_description(value);
        }
        if value.contains("://") {
            if let Ok(parsed) = url::Url::parse(value) {
                if !parsed.username().is_empty()
                    || parsed.password().is_some()
                    || parsed.query().is_some()
                    || parsed.fragment().is_some()
                {
                    return "<redacted-url>".into();
                }
            } else if value.contains('?') || value.contains('@') || value.contains('#') {
                return "<redacted-url>".into();
            }
        }
        value.to_string()
    }
}

fn endpoint_description(value: &str) -> String {
    let Ok(parsed) = url::Url::parse(value) else {
        return "<configured-endpoint>".into();
    };
    match parsed.scheme() {
        "http" => "<custom-http-endpoint>".into(),
        "https" => "<custom-https-endpoint>".into(),
        _ => "<configured-endpoint>".into(),
    }
}

#[cfg(test)]
mod tests;
