//! `MongoDB` change stream resume token persistence.
//!
//! Resume tokens are opaque BSON documents that allow a change stream
//! to be resumed from a specific position. This module provides a
//! pluggable storage trait and two implementations:
//!
//! - [`FileResumeTokenStore`]: Persists to a local file (embedded/test use)
//! - [`MongoResumeTokenStore`]: Persists to a dedicated `MongoDB` collection
//!   (production use, feature-gated behind `mongodb-cdc`)
//!
//! # Resume Token Semantics
//!
//! - Track `postBatchResumeToken` from every `getMore` response, **not**
//!   just individual event `_id` fields. This prevents unnecessary oplog
//!   re-scanning across empty `getMore` batches.
//! - On startup: if a persisted token exists, open with `resumeAfter`.
//! - On `invalidate` events, switch to `startAfter` (cannot `resumeAfter`
//!   an invalidate token).

use std::path::PathBuf;

use crate::error::ConnectorError;

/// An opaque resume token from a `MongoDB` change stream.
///
/// Wraps the serialized JSON representation of the `_id` field from
/// a change event or `postBatchResumeToken` from a `getMore` response.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct ResumeToken {
    /// JSON-serialized token data.
    data: String,
}

impl ResumeToken {
    /// Creates a new resume token from a JSON string.
    #[must_use]
    pub fn new(data: String) -> Self {
        Self { data }
    }

    /// Returns the JSON representation of the token.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.data
    }

    /// Consumes the token and returns the inner JSON string.
    #[must_use]
    pub fn into_inner(self) -> String {
        self.data
    }
}

impl std::fmt::Display for ResumeToken {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.data)
    }
}

/// Errors from resume token store operations.
#[derive(Debug, thiserror::Error)]
pub enum ResumeTokenStoreError {
    /// I/O error reading/writing the token.
    #[error("resume token I/O error: {0}")]
    Io(#[from] std::io::Error),

    /// Serialization error.
    #[error("resume token serialization error: {0}")]
    Serialization(String),

    /// Store-specific error (e.g., `MongoDB` write failure).
    #[error("resume token store error: {0}")]
    Store(String),
}

/// Trait for pluggable resume token persistence.
///
/// Implementations must be `Send + Sync` for use from async contexts.
///
/// # Cancellation Safety
///
/// Implementations should ensure that `save` is atomic (write-then-rename
/// for files, or upsert for databases) so that a cancelled future does
/// not leave a partially-written token.
#[async_trait::async_trait]
pub trait ResumeTokenStore: Send + Sync {
    /// Loads the most recently persisted resume token, if any.
    ///
    /// # Errors
    ///
    /// Returns `ResumeTokenStoreError` on I/O or deserialization failure.
    async fn load(&self) -> Result<Option<ResumeToken>, ResumeTokenStoreError>;

    /// Persists a resume token, overwriting any previous value.
    ///
    /// # Errors
    ///
    /// Returns `ResumeTokenStoreError` on I/O or serialization failure.
    async fn save(&self, token: &ResumeToken) -> Result<(), ResumeTokenStoreError>;

    /// Clears the persisted resume token.
    ///
    /// # Errors
    ///
    /// Returns `ResumeTokenStoreError` on I/O failure.
    async fn clear(&self) -> Result<(), ResumeTokenStoreError>;
}

/// File-based resume token store for embedded and test use.
///
/// Stores the token as a JSON file at the configured path. Uses
/// atomic write-then-rename to prevent partial writes.
#[derive(Debug, Clone)]
pub struct FileResumeTokenStore {
    /// Path to the token file.
    path: PathBuf,
}

impl FileResumeTokenStore {
    /// Creates a new file-based store at the given path.
    #[must_use]
    pub fn new(path: PathBuf) -> Self {
        Self { path }
    }
}

#[async_trait::async_trait]
impl ResumeTokenStore for FileResumeTokenStore {
    async fn load(&self) -> Result<Option<ResumeToken>, ResumeTokenStoreError> {
        let path = self.path.clone();
        tokio::task::spawn_blocking(move || {
            if !path.exists() {
                return Ok(None);
            }
            let data = std::fs::read_to_string(&path)?;
            let trimmed = data.trim();
            if trimmed.is_empty() {
                return Ok(None);
            }
            Ok(Some(ResumeToken::new(trimmed.to_string())))
        })
        .await
        .map_err(|e| ResumeTokenStoreError::Store(format!("spawn_blocking join: {e}")))?
    }

    async fn save(&self, token: &ResumeToken) -> Result<(), ResumeTokenStoreError> {
        let path = self.path.clone();
        let data = token.as_str().to_string();
        tokio::task::spawn_blocking(move || {
            // Write to a temp file then rename for atomicity.
            // On Windows, rename fails if the destination exists.
            let tmp_path = path.with_extension("tmp");
            std::fs::write(&tmp_path, &data)?;
            #[cfg(windows)]
            {
                let _ = std::fs::remove_file(&path);
            }
            std::fs::rename(&tmp_path, &path)?;
            Ok(())
        })
        .await
        .map_err(|e| ResumeTokenStoreError::Store(format!("spawn_blocking join: {e}")))?
    }

    async fn clear(&self) -> Result<(), ResumeTokenStoreError> {
        let path = self.path.clone();
        tokio::task::spawn_blocking(move || {
            if path.exists() {
                std::fs::remove_file(&path)?;
            }
            Ok(())
        })
        .await
        .map_err(|e| ResumeTokenStoreError::Store(format!("spawn_blocking join: {e}")))?
    }
}

/// MongoDB-backed resume token store (feature-gated behind `mongodb-cdc`).
///
/// Persists the resume token to a dedicated collection using an upsert
/// on a fixed document ID. This ensures exactly one token document exists
/// per source instance.
#[cfg(feature = "mongodb-cdc")]
#[derive(Debug, Clone)]
pub struct MongoResumeTokenStore {
    /// The `MongoDB` collection used for token storage.
    collection: mongodb::Collection<mongodb::bson::Document>,
    /// Unique identifier for this source instance.
    source_id: String,
}

#[cfg(feature = "mongodb-cdc")]
impl MongoResumeTokenStore {
    /// Creates a new MongoDB-backed token store.
    #[must_use]
    pub fn new(
        collection: mongodb::Collection<mongodb::bson::Document>,
        source_id: String,
    ) -> Self {
        Self {
            collection,
            source_id,
        }
    }
}

#[cfg(feature = "mongodb-cdc")]
#[async_trait::async_trait]
impl ResumeTokenStore for MongoResumeTokenStore {
    async fn load(&self) -> Result<Option<ResumeToken>, ResumeTokenStoreError> {
        use mongodb::bson::doc;

        let filter = doc! { "_id": &self.source_id };
        let result = self
            .collection
            .find_one(filter)
            .await
            .map_err(|e| ResumeTokenStoreError::Store(e.to_string()))?;

        match result {
            Some(doc) => {
                let token_str = doc
                    .get_str("token")
                    .map_err(|e| ResumeTokenStoreError::Serialization(e.to_string()))?;
                Ok(Some(ResumeToken::new(token_str.to_string())))
            }
            None => Ok(None),
        }
    }

    async fn save(&self, token: &ResumeToken) -> Result<(), ResumeTokenStoreError> {
        use mongodb::bson::doc;

        let filter = doc! { "_id": &self.source_id };
        let update = doc! {
            "$set": {
                "token": token.as_str(),
                "updated_at": mongodb::bson::DateTime::now(),
            }
        };
        let opts = mongodb::options::UpdateOptions::builder()
            .upsert(true)
            .build();

        self.collection
            .update_one(filter, update)
            .with_options(opts)
            .await
            .map_err(|e| ResumeTokenStoreError::Store(e.to_string()))?;

        Ok(())
    }

    async fn clear(&self) -> Result<(), ResumeTokenStoreError> {
        use mongodb::bson::doc;

        let filter = doc! { "_id": &self.source_id };
        self.collection
            .delete_one(filter)
            .await
            .map_err(|e| ResumeTokenStoreError::Store(e.to_string()))?;

        Ok(())
    }
}

/// Configuration for resume token persistence.
#[derive(Debug, Clone, Default, serde::Serialize, serde::Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ResumeTokenStoreConfig {
    /// File-based token storage (for embedded/test use).
    File {
        /// Path to the token file.
        path: String,
    },
    /// `MongoDB`-based token storage (production).
    Mongo {
        /// Collection name for token storage (uses the same connection).
        collection: String,
        /// Unique source instance identifier.
        source_id: String,
    },
    /// In-memory only (no persistence across restarts).
    #[default]
    Memory,
}

/// In-memory resume token store (no persistence, for testing).
#[derive(Debug, Default)]
pub struct InMemoryResumeTokenStore {
    /// `parking_lot::Mutex` is safe here — none of the impl methods hold
    /// the guard across an `.await`. The trait is async for parity with
    /// the MongoDB-backed store which does actually await network I/O.
    token: parking_lot::Mutex<Option<ResumeToken>>,
}

impl InMemoryResumeTokenStore {
    /// Creates a new in-memory store.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }
}

#[async_trait::async_trait]
impl ResumeTokenStore for InMemoryResumeTokenStore {
    async fn load(&self) -> Result<Option<ResumeToken>, ResumeTokenStoreError> {
        Ok(self.token.lock().clone())
    }

    async fn save(&self, token: &ResumeToken) -> Result<(), ResumeTokenStoreError> {
        *self.token.lock() = Some(token.clone());
        Ok(())
    }

    async fn clear(&self) -> Result<(), ResumeTokenStoreError> {
        *self.token.lock() = None;
        Ok(())
    }
}

impl From<ResumeTokenStoreError> for ConnectorError {
    fn from(e: ResumeTokenStoreError) -> Self {
        ConnectorError::Internal(format!("resume token store: {e}"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_resume_token_new() {
        let token = ResumeToken::new(r#"{"_data": "abc123"}"#.to_string());
        assert_eq!(token.as_str(), r#"{"_data": "abc123"}"#);
    }

    #[test]
    fn test_resume_token_display() {
        let token = ResumeToken::new("tok123".to_string());
        assert_eq!(token.to_string(), "tok123");
    }

    #[test]
    fn test_resume_token_into_inner() {
        let token = ResumeToken::new("data".to_string());
        assert_eq!(token.into_inner(), "data");
    }

    #[test]
    fn test_resume_token_store_config_default() {
        assert!(matches!(
            ResumeTokenStoreConfig::default(),
            ResumeTokenStoreConfig::Memory
        ));
    }

    #[tokio::test]
    async fn test_file_store_roundtrip() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("token.json");
        let store = FileResumeTokenStore::new(path);

        // Empty initially.
        assert!(store.load().await.unwrap().is_none());

        // Save and load.
        let token = ResumeToken::new(r#"{"_data": "test"}"#.to_string());
        store.save(&token).await.unwrap();
        let loaded = store.load().await.unwrap().unwrap();
        assert_eq!(loaded.as_str(), token.as_str());

        // Clear.
        store.clear().await.unwrap();
        assert!(store.load().await.unwrap().is_none());
    }

    #[tokio::test]
    async fn test_in_memory_store_roundtrip() {
        let store = InMemoryResumeTokenStore::new();

        assert!(store.load().await.unwrap().is_none());

        let token = ResumeToken::new("test_token".to_string());
        store.save(&token).await.unwrap();
        let loaded = store.load().await.unwrap().unwrap();
        assert_eq!(loaded.as_str(), "test_token");

        store.clear().await.unwrap();
        assert!(store.load().await.unwrap().is_none());
    }
}
