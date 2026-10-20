//! Bind a process's shuffle fabric to its pipeline identity, so peers compiled from another
//! binary or catalog refuse to connect instead of exchanging frames they encode differently.

use super::*;
use crate::checkpoint::PipelineIdentity;

impl ShuffleSender {
    /// Bind both directions of this process's fabric to `identity`. A receiver refuses the
    /// handshake of a sender bound to a different identity or to none, so the first exchange
    /// fails before any frame crosses. Changing the identity cancels the streams and handshakes
    /// opened under the previous one; sequence domains stay scoped to the assignment.
    ///
    /// # Errors
    /// Rejects a non-canonical identity and a sender and receiver of different processes.
    pub fn install_pipeline_identity_pair(
        &self,
        receiver: &ShuffleReceiver,
        identity: &PipelineIdentity,
    ) -> io::Result<bool> {
        let digest = identity_digest(identity)?;
        // Same lock order as assignment and topology installation, which open and admit streams.
        let _sender_assignment = self.assignment.write();
        let _receiver_assignment = receiver.assignment.write();
        if self.local_id != receiver.local_id
            || self.sender_incarnation != receiver.receiver_incarnation
        {
            return Err(io::Error::new(
                io::ErrorKind::PermissionDenied,
                "pipeline identity binds one process's own sender and receiver",
            ));
        }
        let mut sender_identity = self.pipeline_identity.write();
        let mut receiver_identity = receiver.pipeline_identity.write();
        if *sender_identity == Some(digest) && *receiver_identity == Some(digest) {
            return Ok(false);
        }
        *sender_identity = Some(digest);
        *receiver_identity = Some(digest);
        for (scope, lease) in [
            (&self.scope_cancel, &self.process_lease),
            (&receiver.scope_cancel, &receiver.process_lease),
        ] {
            if !scope.read().is_cancelled() {
                rotate_scope_token(scope, lease, true);
            }
        }
        self.pool.lock().clear();
        self.connect_locks.lock().clear();
        receiver.pending_handshakes.clear();
        Ok(true)
    }
}

fn identity_digest(identity: &PipelineIdentity) -> io::Result<[u8; 32]> {
    if !identity.is_canonical() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "shuffle pipeline identity is not canonical",
        ));
    }
    let mut digest = [0; 32];
    for (byte, encoded) in digest
        .iter_mut()
        .zip(identity.sha256.as_bytes().as_chunks::<2>().0)
    {
        let encoded = std::str::from_utf8(encoded).map_err(io::Error::other)?;
        *byte = u8::from_str_radix(encoded, 16).map_err(io::Error::other)?;
    }
    Ok(digest)
}
