//! Versioned envelope shared by coordinated-commit descriptors; the payload is
//! sink-specific (Iceberg data files, Delta add actions).

use serde::de::DeserializeOwned;
use serde::Serialize;

use crate::error::ConnectorError;

const VERSION: u32 = 1;

#[derive(Serialize, serde::Deserialize)]
struct Envelope<T> {
    version: u32,
    payload: T,
}

pub(super) fn encode<T: Serialize>(payload: T) -> Result<Vec<u8>, ConnectorError> {
    serde_json::to_vec(&Envelope {
        version: VERSION,
        payload,
    })
    .map_err(|e| ConnectorError::WriteError(format!("encode commit descriptor: {e}")))
}

/// Rejects an unknown version.
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
