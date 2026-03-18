//! Encryption at rest using AES-256-GCM.
//!
//! Provides a transparent [`EncryptionLayer`] that encrypts data on write
//! and decrypts on read, suitable for checkpoint files and WAL segments.
//!
//! The encryption key is a 32-byte AES-256 key, typically loaded from
//! configuration or an environment variable.

use aes_gcm::aead::Aead;
use aes_gcm::{Aes256Gcm, Key, KeyInit, Nonce};

/// AES-256-GCM nonce size in bytes (96 bits).
const NONCE_SIZE: usize = 12;

/// Magic header prepended to encrypted data for format detection.
const ENCRYPTED_MAGIC: &[u8; 4] = b"LENC";

/// Transparent encryption/decryption layer for data at rest.
///
/// Wraps AES-256-GCM with random nonces. The encrypted format is:
/// ```text
/// [LENC magic (4 bytes)] [nonce (12 bytes)] [ciphertext + tag ...]
/// ```
#[derive(Clone)]
pub struct EncryptionLayer {
    cipher: Aes256Gcm,
}

impl EncryptionLayer {
    /// Create a new encryption layer from a 32-byte key.
    ///
    /// # Errors
    ///
    /// Returns `EncryptionError::InvalidKeyLength` if the key is not
    /// exactly 32 bytes.
    pub fn new(key: &[u8]) -> Result<Self, EncryptionError> {
        if key.len() != 32 {
            return Err(EncryptionError::InvalidKeyLength {
                expected: 32,
                actual: key.len(),
            });
        }
        let key = Key::<Aes256Gcm>::from_slice(key);
        let cipher = Aes256Gcm::new(key);
        Ok(Self { cipher })
    }

    /// Create an encryption layer from a hex-encoded key string.
    ///
    /// The hex string must decode to exactly 32 bytes (64 hex chars).
    pub fn from_hex(hex_key: &str) -> Result<Self, EncryptionError> {
        let bytes = hex_decode(hex_key)?;
        Self::new(&bytes)
    }

    /// Encrypt plaintext data.
    ///
    /// Returns `[magic][nonce][ciphertext+tag]`.
    pub fn encrypt(&self, plaintext: &[u8]) -> Result<Vec<u8>, EncryptionError> {
        let nonce_bytes = generate_nonce();
        let nonce = Nonce::from_slice(&nonce_bytes);

        let ciphertext = self
            .cipher
            .encrypt(nonce, plaintext)
            .map_err(|e| EncryptionError::EncryptFailed(e.to_string()))?;

        let mut output = Vec::with_capacity(ENCRYPTED_MAGIC.len() + NONCE_SIZE + ciphertext.len());
        output.extend_from_slice(ENCRYPTED_MAGIC);
        output.extend_from_slice(&nonce_bytes);
        output.extend_from_slice(&ciphertext);
        Ok(output)
    }

    /// Decrypt data that was encrypted with [`encrypt`](Self::encrypt).
    ///
    /// Validates the magic header, extracts the nonce, and decrypts.
    pub fn decrypt(&self, encrypted: &[u8]) -> Result<Vec<u8>, DecryptionError> {
        let header_size = ENCRYPTED_MAGIC.len() + NONCE_SIZE;
        if encrypted.len() < header_size {
            return Err(DecryptionError::TooShort {
                len: encrypted.len(),
                min: header_size,
            });
        }

        // Validate magic
        if &encrypted[..4] != ENCRYPTED_MAGIC {
            return Err(DecryptionError::NotEncrypted);
        }

        let nonce_bytes = &encrypted[4..4 + NONCE_SIZE];
        let ciphertext = &encrypted[4 + NONCE_SIZE..];
        let nonce = Nonce::from_slice(nonce_bytes);

        self.cipher
            .decrypt(nonce, ciphertext)
            .map_err(|e| DecryptionError::DecryptFailed(e.to_string()))
    }

    /// Check whether the given data appears to be encrypted (has the magic header).
    pub fn is_encrypted(data: &[u8]) -> bool {
        data.len() >= ENCRYPTED_MAGIC.len() && &data[..4] == ENCRYPTED_MAGIC
    }
}

impl std::fmt::Debug for EncryptionLayer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("EncryptionLayer")
            .field("algorithm", &"AES-256-GCM")
            .finish()
    }
}

/// Encryption errors.
#[derive(Debug, thiserror::Error)]
pub enum EncryptionError {
    /// The encryption key has an invalid length.
    #[error("invalid key length: expected {expected} bytes, got {actual}")]
    InvalidKeyLength {
        /// Expected number of bytes.
        expected: usize,
        /// Actual number of bytes.
        actual: usize,
    },

    /// Encryption operation failed.
    #[error("encryption failed: {0}")]
    EncryptFailed(String),

    /// Hex decoding of the key failed.
    #[error("invalid hex key: {0}")]
    InvalidHexKey(String),
}

/// Decryption errors.
#[derive(Debug, thiserror::Error)]
pub enum DecryptionError {
    /// The encrypted data is too short to contain the header.
    #[error("encrypted data too short ({len} bytes, minimum {min})")]
    TooShort {
        /// Actual data length.
        len: usize,
        /// Minimum required length.
        min: usize,
    },

    /// The data does not have the encryption magic header.
    #[error("data is not encrypted (missing LENC header)")]
    NotEncrypted,

    /// Decryption failed (wrong key, corrupted data, or tampered ciphertext).
    #[error("decryption failed: {0}")]
    DecryptFailed(String),
}

/// Generate a random 12-byte nonce.
fn generate_nonce() -> [u8; NONCE_SIZE] {
    use aes_gcm::aead::rand_core::RngCore;
    use aes_gcm::aead::OsRng;

    let mut nonce = [0u8; NONCE_SIZE];
    OsRng.fill_bytes(&mut nonce);
    nonce
}

/// Decode a hex string to bytes.
fn hex_decode(hex: &str) -> Result<Vec<u8>, EncryptionError> {
    if !hex.len().is_multiple_of(2) {
        return Err(EncryptionError::InvalidHexKey(
            "odd number of hex characters".to_string(),
        ));
    }
    (0..hex.len())
        .step_by(2)
        .map(|i| {
            u8::from_str_radix(&hex[i..i + 2], 16)
                .map_err(|e| EncryptionError::InvalidHexKey(e.to_string()))
        })
        .collect()
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    fn test_key() -> [u8; 32] {
        // Deterministic test key (NOT for production use)
        let mut key = [0u8; 32];
        for (i, byte) in key.iter_mut().enumerate() {
            #[allow(clippy::cast_possible_truncation)]
            {
                *byte = (i as u8).wrapping_mul(7).wrapping_add(42);
            }
        }
        key
    }

    #[test]
    fn test_roundtrip() {
        let layer = EncryptionLayer::new(&test_key()).unwrap();
        let plaintext = b"Hello, LaminarDB! This is secret data.";

        let encrypted = layer.encrypt(plaintext).unwrap();
        assert_ne!(encrypted.as_slice(), plaintext.as_slice());

        let decrypted = layer.decrypt(&encrypted).unwrap();
        assert_eq!(decrypted, plaintext);
    }

    #[test]
    fn test_empty_plaintext() {
        let layer = EncryptionLayer::new(&test_key()).unwrap();
        let encrypted = layer.encrypt(b"").unwrap();
        let decrypted = layer.decrypt(&encrypted).unwrap();
        assert!(decrypted.is_empty());
    }

    #[test]
    fn test_large_plaintext() {
        let layer = EncryptionLayer::new(&test_key()).unwrap();
        let plaintext = vec![0xAB; 1_000_000]; // 1MB
        let encrypted = layer.encrypt(&plaintext).unwrap();
        let decrypted = layer.decrypt(&encrypted).unwrap();
        assert_eq!(decrypted, plaintext);
    }

    #[test]
    fn test_different_nonces() {
        let layer = EncryptionLayer::new(&test_key()).unwrap();
        let plaintext = b"same data";

        let encrypted1 = layer.encrypt(plaintext).unwrap();
        let encrypted2 = layer.encrypt(plaintext).unwrap();

        // Same plaintext should produce different ciphertext (different nonces)
        assert_ne!(encrypted1, encrypted2);

        // Both should decrypt correctly
        assert_eq!(layer.decrypt(&encrypted1).unwrap(), plaintext);
        assert_eq!(layer.decrypt(&encrypted2).unwrap(), plaintext);
    }

    #[test]
    fn test_wrong_key_fails_decrypt() {
        let layer1 = EncryptionLayer::new(&test_key()).unwrap();
        let mut other_key = test_key();
        other_key[0] ^= 0xFF;
        let layer2 = EncryptionLayer::new(&other_key).unwrap();

        let encrypted = layer1.encrypt(b"secret").unwrap();
        assert!(layer2.decrypt(&encrypted).is_err());
    }

    #[test]
    fn test_tampered_ciphertext_fails() {
        let layer = EncryptionLayer::new(&test_key()).unwrap();
        let mut encrypted = layer.encrypt(b"secret").unwrap();

        // Flip a byte in the ciphertext portion
        let last = encrypted.len() - 1;
        encrypted[last] ^= 0xFF;

        assert!(layer.decrypt(&encrypted).is_err());
    }

    #[test]
    fn test_invalid_key_length() {
        assert!(EncryptionLayer::new(&[0u8; 16]).is_err());
        assert!(EncryptionLayer::new(&[0u8; 31]).is_err());
        assert!(EncryptionLayer::new(&[0u8; 33]).is_err());
    }

    #[test]
    fn test_is_encrypted() {
        let layer = EncryptionLayer::new(&test_key()).unwrap();
        let encrypted = layer.encrypt(b"data").unwrap();

        assert!(EncryptionLayer::is_encrypted(&encrypted));
        assert!(!EncryptionLayer::is_encrypted(b"not encrypted"));
        assert!(!EncryptionLayer::is_encrypted(b""));
        assert!(!EncryptionLayer::is_encrypted(b"LEN")); // too short
    }

    #[test]
    fn test_not_encrypted_error() {
        let layer = EncryptionLayer::new(&test_key()).unwrap();
        match layer.decrypt(b"not encrypted data here!") {
            Err(DecryptionError::NotEncrypted) => {}
            other => panic!("expected NotEncrypted, got {other:?}"),
        }
    }

    #[test]
    fn test_too_short_error() {
        let layer = EncryptionLayer::new(&test_key()).unwrap();
        match layer.decrypt(b"LENC") {
            Err(DecryptionError::TooShort { .. }) => {}
            other => panic!("expected TooShort, got {other:?}"),
        }
    }

    #[test]
    fn test_from_hex() {
        let hex_key = "000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f";
        let layer = EncryptionLayer::from_hex(hex_key).unwrap();

        let encrypted = layer.encrypt(b"test").unwrap();
        let decrypted = layer.decrypt(&encrypted).unwrap();
        assert_eq!(decrypted, b"test");
    }

    #[test]
    fn test_from_hex_invalid() {
        assert!(EncryptionLayer::from_hex("not-hex").is_err());
        assert!(EncryptionLayer::from_hex("aabb").is_err()); // too short
        assert!(EncryptionLayer::from_hex("abc").is_err()); // odd length
    }

    #[test]
    fn test_debug_does_not_leak_key() {
        let layer = EncryptionLayer::new(&test_key()).unwrap();
        let debug = format!("{layer:?}");
        assert!(debug.contains("AES-256-GCM"));
        // Should NOT contain raw key bytes
        assert!(!debug.contains("42"));
    }
}
