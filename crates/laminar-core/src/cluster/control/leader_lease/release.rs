//! Advisory record that a leader term's owner fenced its local grant and gave the term up.
//!
//! It lives outside the authority record chain, whose readers reject any body they cannot
//! re-encode exactly. Binaries that predate it never read it and keep taking a term over only
//! after observing it for a full TTL.

use super::{
    Deserialize, LeaderAuthorityRecord, LeaderLeaseOwner, LeaderLeaseStore, LeaseError,
    ObjectStoreExt, OsPath, PutMode, PutOptions, PutPayload, Serialize,
};

const LEADER_LEASE_RELEASE_PATH: &str = "control/leader-lease-release/v1.json";
const LEADER_LEASE_RELEASE_VERSION: u32 = 1;
const MAX_LEADER_LEASE_RELEASE_BYTES: u64 = 1024;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct LeaderLeaseRelease {
    version: u32,
    owner: LeaderLeaseOwner,
    token: u64,
}

impl LeaderLeaseStore {
    /// Record that `owner` gave up its term `token`. The caller must already have fenced its
    /// local grant; fencing tokens are never reused, so a stale record cannot name a later term.
    ///
    /// # Errors
    /// Fails on invalid ownership, encoding, or object-store I/O.
    #[cfg(feature = "cluster")]
    pub(super) async fn publish_release(
        &self,
        owner: &LeaderLeaseOwner,
        token: u64,
    ) -> Result<(), LeaseError> {
        owner.validate()?;
        let body = serde_json::to_vec(&LeaderLeaseRelease {
            version: LEADER_LEASE_RELEASE_VERSION,
            owner: owner.clone(),
            token,
        })?;
        self.store
            .put_opts(
                &OsPath::from(LEADER_LEASE_RELEASE_PATH),
                PutPayload::from(body),
                PutOptions {
                    mode: PutMode::Overwrite,
                    ..PutOptions::default()
                },
            )
            .await
            .map(|_| ())
            .map_err(|error| LeaseError::Io(error.to_string()))
    }

    /// Whether the current term of `record` was released and owns no admitted checkpoint
    /// attempt, so a rival may take it over without observing it for a full TTL. An admitted
    /// attempt keeps the settlement window that the TTL observation would have given it.
    ///
    /// # Errors
    /// Fails on object-store I/O or a malformed release record.
    pub(super) async fn term_released(
        &self,
        record: &LeaderAuthorityRecord,
    ) -> Result<bool, LeaseError> {
        if record
            .active_checkpoint_artifact_leader_proof
            .as_ref()
            .is_some_and(|proof| record.lease.matches_proof(proof))
        {
            return Ok(false);
        }
        let result = match self
            .store
            .get(&OsPath::from(LEADER_LEASE_RELEASE_PATH))
            .await
        {
            Ok(result) => result,
            Err(object_store::Error::NotFound { .. }) => return Ok(false),
            Err(error) => return Err(LeaseError::Io(error.to_string())),
        };
        if result.meta.size == 0 || result.meta.size > MAX_LEADER_LEASE_RELEASE_BYTES {
            return Err(LeaseError::Invalid(format!(
                "leader lease release is {} bytes; maximum is {MAX_LEADER_LEASE_RELEASE_BYTES}",
                result.meta.size
            )));
        }
        let bytes = result
            .bytes()
            .await
            .map_err(|error| LeaseError::Io(error.to_string()))?;
        let release: LeaderLeaseRelease = serde_json::from_slice(&bytes)?;
        if release.version != LEADER_LEASE_RELEASE_VERSION {
            return Err(LeaseError::Invalid(format!(
                "unsupported leader lease release version {}",
                release.version
            )));
        }
        Ok(release.owner == record.lease.owner && release.token == record.lease.token)
    }
}
