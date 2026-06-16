//! Versioned envelope shared by coordinated-commit descriptors. The payload is
//! sink-specific (Iceberg data files, Delta add actions); only the version
//! framing and the encode/decode error contract are shared here.

use serde::de::DeserializeOwned;
use serde::Serialize;

use crate::error::ConnectorError;

const VERSION: u32 = 1;

#[derive(Serialize, serde::Deserialize)]
struct Envelope<T> {
    version: u32,
    payload: T,
}

/// Serialize `payload` into a versioned commit descriptor.
///
/// # Errors
/// Returns `ConnectorError::WriteError` on serialization failure.
pub(super) fn encode<T: Serialize>(payload: T) -> Result<Vec<u8>, ConnectorError> {
    serde_json::to_vec(&Envelope {
        version: VERSION,
        payload,
    })
    .map_err(|e| ConnectorError::WriteError(format!("encode commit descriptor: {e}")))
}

/// Deserialize a commit descriptor, rejecting an unknown version.
///
/// # Errors
/// Returns `ConnectorError::TransactionError` on a version mismatch or malformed
/// bytes.
pub(super) fn decode<T: DeserializeOwned>(bytes: &[u8]) -> Result<T, ConnectorError> {
    let envelope: Envelope<T> = serde_json::from_slice(bytes)
        .map_err(|e| ConnectorError::TransactionError(format!("decode commit descriptor: {e}")))?;
    if envelope.version != VERSION {
        return Err(ConnectorError::TransactionError(format!(
            "unsupported commit descriptor version {}",
            envelope.version
        )));
    }
    Ok(envelope.payload)
}
