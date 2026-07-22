//! Durable Delta coordinated-commit descriptor.

use deltalake::kernel::Add;
use serde::{Deserialize, Serialize};

use crate::error::ConnectorError;

const VERSION: u32 = 2;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(super) struct DeltaTableBinding {
    pub table_id: String,
    pub write_metadata_sha256: String,
}

#[derive(Debug, Clone)]
pub(super) struct DeltaCommitDescriptor {
    pub binding: DeltaTableBinding,
    pub adds: Vec<Add>,
}

#[derive(Serialize, Deserialize)]
struct Envelope {
    version: u32,
    binding: DeltaTableBinding,
    adds: Vec<Add>,
}

pub(super) fn encode(binding: &DeltaTableBinding, adds: &[Add]) -> Result<Vec<u8>, ConnectorError> {
    serde_json::to_vec(&Envelope {
        version: VERSION,
        binding: binding.clone(),
        adds: adds.to_vec(),
    })
    .map_err(|error| ConnectorError::WriteError(format!("encode commit descriptor: {error}")))
}

pub(super) fn encoded_add_array_len(adds: &[Add]) -> Result<usize, ConnectorError> {
    serde_json::to_vec(adds)
        .map(|bytes| bytes.len())
        .map_err(|error| ConnectorError::WriteError(format!("encode Delta Adds: {error}")))
}

pub(super) fn decode(bytes: &[u8]) -> Result<DeltaCommitDescriptor, ConnectorError> {
    let envelope: Envelope = serde_json::from_slice(bytes).map_err(|error| {
        ConnectorError::TransactionError(format!("decode commit descriptor: {error}"))
    })?;
    if envelope.version != VERSION {
        return Err(ConnectorError::TransactionError(format!(
            "unsupported Delta commit descriptor version {} (this build supports {VERSION})",
            envelope.version
        )));
    }
    Ok(DeltaCommitDescriptor {
        binding: envelope.binding,
        adds: envelope.adds,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn binding() -> DeltaTableBinding {
        DeltaTableBinding {
            table_id: "018f0000-0000-7000-8000-000000000001".into(),
            write_metadata_sha256: "11".repeat(32),
        }
    }

    #[test]
    fn roundtrip_empty_descriptor_and_reject_non_current_versions() {
        let bytes = encode(&binding(), &[]).unwrap();
        let decoded = decode(&bytes).unwrap();
        assert_eq!(decoded.binding, binding());
        assert!(decoded.adds.is_empty());

        let obsolete =
            br#"{"version":1,"binding":{"table_id":"t","write_metadata_sha256":"00"},"adds":[]}"#;
        let error = decode(obsolete).unwrap_err().to_string();
        assert!(error.contains("version 1"), "got: {error}");

        let future =
            br#"{"version":999,"binding":{"table_id":"t","write_metadata_sha256":"00"},"adds":[]}"#;
        let error = decode(future).unwrap_err().to_string();
        assert!(error.contains("version 999"), "got: {error}");
    }
}
