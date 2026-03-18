//! Key Management for encryption at rest.
//!
//! Provides a [`KeyManager`] trait and two implementations:
//!
//! - [`FileKeyManager`]: reads key material from files in a directory
//! - [`EnvKeyManager`]: reads key material from environment variables

use std::collections::HashMap;
use std::path::{Path, PathBuf};

use crate::identity::AuthError;

/// A cryptographic key with metadata.
#[derive(Clone)]
pub struct Key {
    /// Unique identifier for this key (e.g., "primary", "backup-2024").
    pub id: String,
    /// Raw key bytes.
    pub material: Vec<u8>,
    /// Key algorithm hint (e.g., "AES-256-GCM", "ChaCha20Poly1305").
    pub algorithm: String,
}

impl std::fmt::Debug for Key {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Key")
            .field("id", &self.id)
            .field("algorithm", &self.algorithm)
            .field("material", &"[REDACTED]")
            .finish()
    }
}

/// Trait for key management backends.
///
/// Implementations provide key retrieval for encryption at rest.
#[async_trait::async_trait]
pub trait KeyManager: Send + Sync {
    /// Retrieve a key by its ID.
    ///
    /// Returns `AuthError::Configuration` if the key does not exist.
    async fn get_key(&self, key_id: &str) -> Result<Key, AuthError>;

    /// List all available key IDs.
    async fn list_key_ids(&self) -> Result<Vec<String>, AuthError>;
}

/// Reads key material from files in a configured directory.
///
/// Each file is named `{key_id}.key` and contains raw key bytes
/// (binary) or base64-encoded key material (if the file ends with
/// a newline, it is trimmed and decoded as base64).
///
/// The algorithm is read from a companion `{key_id}.algorithm` file
/// if it exists, otherwise defaults to `"AES-256-GCM"`.
#[derive(Debug)]
pub struct FileKeyManager {
    directory: PathBuf,
}

impl FileKeyManager {
    /// Create a new file-based key manager reading from the given directory.
    pub fn new(directory: impl Into<PathBuf>) -> Self {
        Self {
            directory: directory.into(),
        }
    }

    fn key_path(&self, key_id: &str) -> PathBuf {
        self.directory.join(format!("{key_id}.key"))
    }

    fn algorithm_path(&self, key_id: &str) -> PathBuf {
        self.directory.join(format!("{key_id}.algorithm"))
    }
}

#[async_trait::async_trait]
impl KeyManager for FileKeyManager {
    async fn get_key(&self, key_id: &str) -> Result<Key, AuthError> {
        let path = self.key_path(key_id);
        let raw = tokio::fs::read(&path).await.map_err(|e| {
            AuthError::Configuration(format!("failed to read key '{}': {}", path.display(), e))
        })?;

        // Try base64 decode; fall back to raw bytes
        let material = decode_key_material(&raw);

        let algorithm = match tokio::fs::read_to_string(self.algorithm_path(key_id)).await {
            Ok(alg) => alg.trim().to_string(),
            Err(_) => "AES-256-GCM".to_string(),
        };

        Ok(Key {
            id: key_id.to_string(),
            material,
            algorithm,
        })
    }

    async fn list_key_ids(&self) -> Result<Vec<String>, AuthError> {
        let mut ids = Vec::new();
        let mut entries = tokio::fs::read_dir(&self.directory).await.map_err(|e| {
            AuthError::Configuration(format!(
                "failed to read key directory '{}': {}",
                self.directory.display(),
                e
            ))
        })?;

        while let Some(entry) = entries
            .next_entry()
            .await
            .map_err(|e| AuthError::Configuration(format!("failed to read directory entry: {e}")))?
        {
            let name = entry.file_name().to_string_lossy().to_string();
            if let Some(id) = name.strip_suffix(".key") {
                ids.push(id.to_string());
            }
        }

        ids.sort();
        Ok(ids)
    }
}

/// Reads key material from environment variables.
///
/// Each key is stored as `{prefix}{KEY_ID}` where the key ID is
/// uppercased and hyphens are replaced with underscores.
///
/// Values are base64-encoded.
///
/// Algorithm is read from `{prefix}{KEY_ID}_ALGORITHM` if set,
/// otherwise defaults to `"AES-256-GCM"`.
#[derive(Debug)]
pub struct EnvKeyManager {
    prefix: String,
    /// Pre-registered key IDs (env vars are not enumerable by pattern).
    known_ids: Vec<String>,
}

impl EnvKeyManager {
    /// Create a new env-based key manager with the given variable prefix.
    ///
    /// # Example
    ///
    /// ```ignore
    /// let mgr = EnvKeyManager::new("LAMINAR_KEY_", &["primary", "backup"]);
    /// // Reads LAMINAR_KEY_PRIMARY, LAMINAR_KEY_BACKUP
    /// ```
    pub fn new(prefix: impl Into<String>, known_ids: &[&str]) -> Self {
        Self {
            prefix: prefix.into(),
            known_ids: known_ids.iter().map(|s| (*s).to_string()).collect(),
        }
    }

    fn env_var_name(&self, key_id: &str) -> String {
        format!("{}{}", self.prefix, key_id.to_uppercase().replace('-', "_"))
    }
}

#[async_trait::async_trait]
impl KeyManager for EnvKeyManager {
    async fn get_key(&self, key_id: &str) -> Result<Key, AuthError> {
        let var_name = self.env_var_name(key_id);
        let encoded = std::env::var(&var_name).map_err(|_| {
            AuthError::Configuration(format!(
                "key '{key_id}' not found (expected env var '{var_name}')"
            ))
        })?;

        let material = base64_decode(&encoded).map_err(|e| {
            AuthError::Configuration(format!("key '{key_id}' has invalid base64: {e}"))
        })?;

        let alg_var = format!("{var_name}_ALGORITHM");
        let algorithm = std::env::var(&alg_var).unwrap_or_else(|_| "AES-256-GCM".to_string());

        Ok(Key {
            id: key_id.to_string(),
            material,
            algorithm,
        })
    }

    async fn list_key_ids(&self) -> Result<Vec<String>, AuthError> {
        Ok(self.known_ids.clone())
    }
}

/// In-memory key manager for testing.
#[derive(Debug, Default)]
pub struct InMemoryKeyManager {
    keys: HashMap<String, Key>,
}

impl InMemoryKeyManager {
    /// Create an empty in-memory key manager.
    pub fn new() -> Self {
        Self::default()
    }

    /// Insert a key.
    pub fn insert(&mut self, key: Key) {
        self.keys.insert(key.id.clone(), key);
    }
}

#[async_trait::async_trait]
impl KeyManager for InMemoryKeyManager {
    async fn get_key(&self, key_id: &str) -> Result<Key, AuthError> {
        self.keys.get(key_id).cloned().ok_or_else(|| {
            AuthError::Configuration(format!("key '{key_id}' not found in memory store"))
        })
    }

    async fn list_key_ids(&self) -> Result<Vec<String>, AuthError> {
        let mut ids: Vec<String> = self.keys.keys().cloned().collect();
        ids.sort();
        Ok(ids)
    }
}

/// Attempt base64 decode; if that fails, return raw bytes.
fn decode_key_material(raw: &[u8]) -> Vec<u8> {
    // Trim trailing newlines (common in files)
    let trimmed = if raw.last() == Some(&b'\n') {
        &raw[..raw.len() - 1]
    } else {
        raw
    };

    // Try UTF-8 + base64
    if let Ok(text) = std::str::from_utf8(trimmed) {
        if let Ok(decoded) = base64_decode(text) {
            return decoded;
        }
    }

    raw.to_vec()
}

/// Decode base64 using the standard alphabet.
fn base64_decode(input: &str) -> Result<Vec<u8>, base64::DecodeError> {
    use base64::Engine;
    base64::engine::general_purpose::STANDARD.decode(input.trim())
}

/// Build a [`KeyManager`] from a configuration specifying the backend type.
pub fn build_key_manager(
    backend: &str,
    directory: Option<&Path>,
    env_prefix: Option<&str>,
    known_ids: &[&str],
) -> Result<Box<dyn KeyManager>, AuthError> {
    match backend {
        "file" => {
            let dir = directory.ok_or_else(|| {
                AuthError::Configuration("file key manager requires a directory path".to_string())
            })?;
            Ok(Box::new(FileKeyManager::new(dir)))
        }
        "env" => {
            let prefix = env_prefix.unwrap_or("LAMINAR_KEY_");
            Ok(Box::new(EnvKeyManager::new(prefix, known_ids)))
        }
        "memory" => Ok(Box::new(InMemoryKeyManager::new())),
        other => Err(AuthError::Configuration(format!(
            "unknown key manager backend: '{other}'"
        ))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_file_key_manager() {
        let dir = tempfile::tempdir().unwrap();

        // Write a base64-encoded key
        let key_bytes = b"super-secret-key-material-32byt";
        let encoded = {
            use base64::Engine;
            base64::engine::general_purpose::STANDARD.encode(key_bytes)
        };
        std::fs::write(dir.path().join("primary.key"), &encoded).unwrap();
        std::fs::write(dir.path().join("primary.algorithm"), "AES-256-GCM").unwrap();

        let mgr = FileKeyManager::new(dir.path());

        let key = mgr.get_key("primary").await.unwrap();
        assert_eq!(key.id, "primary");
        assert_eq!(key.material, key_bytes);
        assert_eq!(key.algorithm, "AES-256-GCM");

        let ids = mgr.list_key_ids().await.unwrap();
        assert_eq!(ids, vec!["primary"]);
    }

    #[tokio::test]
    async fn test_file_key_manager_raw_bytes() {
        let dir = tempfile::tempdir().unwrap();

        // Write raw (non-base64) key bytes
        let key_bytes = vec![0x00, 0x01, 0xFF, 0xFE, 0x80];
        std::fs::write(dir.path().join("raw.key"), &key_bytes).unwrap();

        let mgr = FileKeyManager::new(dir.path());
        let key = mgr.get_key("raw").await.unwrap();
        assert_eq!(key.material, key_bytes);
        assert_eq!(key.algorithm, "AES-256-GCM"); // default
    }

    #[tokio::test]
    async fn test_file_key_manager_missing_key() {
        let dir = tempfile::tempdir().unwrap();
        let mgr = FileKeyManager::new(dir.path());
        assert!(mgr.get_key("nonexistent").await.is_err());
    }

    #[tokio::test]
    async fn test_env_key_manager() {
        let prefix = "LAMINAR_TEST_KEY_";
        let key_bytes = b"env-key-material";
        let encoded = {
            use base64::Engine;
            base64::engine::general_purpose::STANDARD.encode(key_bytes)
        };

        // SAFETY: test isolation — unique prefix avoids collisions.
        unsafe {
            std::env::set_var(format!("{prefix}PRIMARY"), &encoded);
            std::env::set_var(format!("{prefix}PRIMARY_ALGORITHM"), "ChaCha20Poly1305");
        }

        let mgr = EnvKeyManager::new(prefix, &["primary"]);
        let key = mgr.get_key("primary").await.unwrap();
        assert_eq!(key.material, key_bytes);
        assert_eq!(key.algorithm, "ChaCha20Poly1305");

        let ids = mgr.list_key_ids().await.unwrap();
        assert_eq!(ids, vec!["primary"]);

        unsafe {
            std::env::remove_var(format!("{prefix}PRIMARY"));
            std::env::remove_var(format!("{prefix}PRIMARY_ALGORITHM"));
        }
    }

    #[tokio::test]
    async fn test_env_key_manager_missing() {
        let mgr = EnvKeyManager::new("LAMINAR_MISSING_KEY_", &["nope"]);
        assert!(mgr.get_key("nope").await.is_err());
    }

    #[tokio::test]
    async fn test_in_memory_key_manager() {
        let mut mgr = InMemoryKeyManager::new();
        mgr.insert(Key {
            id: "test".to_string(),
            material: vec![1, 2, 3],
            algorithm: "AES-256-GCM".to_string(),
        });

        let key = mgr.get_key("test").await.unwrap();
        assert_eq!(key.material, vec![1, 2, 3]);
        assert!(mgr.get_key("missing").await.is_err());

        let ids = mgr.list_key_ids().await.unwrap();
        assert_eq!(ids, vec!["test"]);
    }

    #[tokio::test]
    async fn test_build_key_manager() {
        let dir = tempfile::tempdir().unwrap();

        let file_mgr = build_key_manager("file", Some(dir.path()), None, &[]).unwrap();
        assert!(file_mgr.list_key_ids().await.unwrap().is_empty());

        let env_mgr = build_key_manager("env", None, Some("TEST_"), &["a"]).unwrap();
        let ids = env_mgr.list_key_ids().await.unwrap();
        assert_eq!(ids, vec!["a"]);

        let mem_mgr = build_key_manager("memory", None, None, &[]).unwrap();
        assert!(mem_mgr.list_key_ids().await.unwrap().is_empty());

        assert!(build_key_manager("unknown", None, None, &[]).is_err());
    }

    #[test]
    fn test_key_debug_redacts_material() {
        let key = Key {
            id: "test".to_string(),
            material: vec![1, 2, 3],
            algorithm: "AES-256-GCM".to_string(),
        };
        let debug = format!("{key:?}");
        assert!(debug.contains("[REDACTED]"));
        assert!(!debug.contains("[1, 2, 3]"));
    }
}
