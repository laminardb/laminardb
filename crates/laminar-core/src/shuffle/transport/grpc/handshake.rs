//! Receiver admission of a shuffle handshake: the requested scope must equal this process's
//! installed assignment, topology, pipeline identity and recovery generation.

use super::*;

impl ShuffleService {
    /// The exact stream fence a handshake request may open, with the scope it was admitted under.
    pub(super) fn admit_handshake(
        &self,
        request: &HandshakeRequest,
    ) -> Result<(StreamFence, ScopeLease), tonic::Status> {
        if request.sender_node_id == 0
            || request.sender_node_id == self.local_id
            || request.assignment_version == 0
        {
            return Err(tonic::Status::failed_precondition(
                "shuffle peers do not share an established assignment scope",
            ));
        }
        let sender_incarnation = parse_uuid(&request.sender_incarnation, "sender incarnation")?;
        let stream_id = parse_uuid(&request.stream_id, "stream id")?;
        let requested_digest = parse_certificate_digest(
            &request.assignment_certificate_digest,
            "assignment certificate digest",
        )?;
        let scope = active_receiver_scope(
            &self.assignment,
            &self.assignment_version,
            &self.recovery_gen,
            &self.scope_cancel,
            &self.process_lease,
        )?;
        if request.topology_version != self.topology_version.load(Ordering::Acquire)
            || parse_topology_fence(request.topology_version, &request.topology_manifest_sha256)?
                != scope.assignment.topology
        {
            return Err(tonic::Status::failed_precondition(
                "shuffle peers do not share the exact topology",
            ));
        }
        let pipeline_identity = (!request.pipeline_identity_sha256.is_empty())
            .then(|| {
                parse_certificate_digest(
                    &request.pipeline_identity_sha256,
                    "pipeline identity digest",
                )
            })
            .transpose()?;
        if pipeline_identity != *self.pipeline_identity.read() {
            return Err(tonic::Status::failed_precondition(
                "shuffle peers are not bound to the same pipeline identity",
            ));
        }
        if request.recovery_gen != scope.recovery_gen {
            return Err(tonic::Status::failed_precondition(
                "shuffle peers do not share a recovery generation",
            ));
        }
        if request.assignment_version != scope.assignment.fence.assignment_version
            || requested_digest != scope.assignment.digest
            || !scope
                .assignment
                .certifies(request.sender_node_id, sender_incarnation)
        {
            return Err(tonic::Status::failed_precondition(
                "shuffle sender is not certified by the installed assignment",
            ));
        }
        let fence = StreamFence {
            sender_node_id: request.sender_node_id,
            sender_incarnation,
            receiver_incarnation: self.receiver_incarnation,
            stream_id,
            assignment_version: scope.assignment.fence.assignment_version,
            assignment_certificate_digest: scope.assignment.digest,
            recovery_gen: scope.recovery_gen,
            topology: scope.assignment.topology,
        };
        Ok((fence, scope))
    }
}
