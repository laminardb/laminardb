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
    "account_key",
    "private_key",
    "sas_token",
    "session_token",
    "client_secret",
    "service_account_key",
];

/// Utility for masking secret values in configuration maps.
pub struct SecretMasker;

impl SecretMasker {
    /// Returns true if the key name suggests it holds a secret value.
    ///
    /// Matches case-insensitively against known secret patterns.
    /// Deliberately does NOT match keys like `aws_access_key_id` (the ID
    /// is not secret) or `aws_region`.
    ///
    /// # Examples
    ///
    /// ```
    /// use laminar_connectors::storage::SecretMasker;
    ///
    /// assert!(SecretMasker::is_secret_key("aws_secret_access_key"));
    /// assert!(SecretMasker::is_secret_key("password"));
    /// assert!(!SecretMasker::is_secret_key("aws_region"));
    /// assert!(!SecretMasker::is_secret_key("aws_access_key_id"));
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
            .map(|(k, v)| {
                if Self::is_secret_key(k) {
                    (k.clone(), "***".to_string())
                } else {
                    (k.clone(), v.clone())
                }
            })
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
            .map(|(k, v)| {
                if Self::is_secret_key(k) {
                    format!("{k}=***")
                } else {
                    format!("{k}={v}")
                }
            })
            .collect::<Vec<_>>()
            .join(", ")
    }
}

#[cfg(test)]
mod tests;
