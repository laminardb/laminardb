//! Versioned envelope shared by coordinated-commit descriptors; the payload is
//! sink-specific (Iceberg data files, Delta add actions).

use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};

use crate::error::ConnectorError;

const VERSION: u32 = 1;

#[derive(Serialize, Deserialize)]
struct Envelope<T> {
    version: u32,
    payload: T,
}

/// Version-only view, so a rolling upgrade can reject a future descriptor by
/// version before its (possibly changed) payload shape is deserialized.
#[derive(Deserialize)]
struct Header {
    version: u32,
}

pub(super) fn encode<T: Serialize>(payload: T) -> Result<Vec<u8>, ConnectorError> {
    serde_json::to_vec(&Envelope {
        version: VERSION,
        payload,
    })
    .map_err(|e| ConnectorError::WriteError(format!("encode commit descriptor: {e}")))
}

/// Rejects an unknown version before touching the payload.
pub(super) fn decode<T: DeserializeOwned>(bytes: &[u8]) -> Result<T, ConnectorError> {
    let header: Header = serde_json::from_slice(bytes).map_err(|e| {
        ConnectorError::TransactionError(format!("decode commit descriptor header: {e}"))
    })?;
    if header.version != VERSION {
        return Err(ConnectorError::TransactionError(format!(
            "unsupported commit descriptor version {} (this build supports {VERSION})",
            header.version
        )));
    }
    let envelope: Envelope<T> = serde_json::from_slice(bytes)
        .map_err(|e| ConnectorError::TransactionError(format!("decode commit descriptor: {e}")))?;
    Ok(envelope.payload)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn roundtrip_then_reject_future_version() {
        let bytes = encode(vec![1u32, 2, 3]).unwrap();
        assert_eq!(decode::<Vec<u32>>(&bytes).unwrap(), vec![1, 2, 3]);

        // A future version is rejected by version, regardless of payload shape.
        let future = br#"{"version":999,"payload":{"unknown":"shape"}}"#;
        let err = decode::<Vec<u32>>(future).unwrap_err();
        assert!(
            err.to_string().contains("unsupported commit descriptor version 999"),
            "got: {err}"
        );
    }
}
