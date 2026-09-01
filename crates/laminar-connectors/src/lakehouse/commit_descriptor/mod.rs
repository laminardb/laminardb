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
mod tests;
