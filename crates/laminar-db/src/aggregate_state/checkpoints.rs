//! Serializable checkpoint shapes for aggregate and window state.
//!
//! `AggStateCheckpoint` is columnar; `GroupCheckpoint` (window/EOWC) and
//! `EmittedCheckpoint` hold one-row IPC tuples.

use arrow::datatypes::Schema;
use xxhash_rust::xxh3::Xxh3;

use crate::error::DbError;

#[derive(Clone, rkyv::Archive, rkyv::Serialize, rkyv::Deserialize)]
pub(crate) struct GroupCheckpoint {
    pub key: Vec<u8>,
    pub acc_states: Vec<Vec<u8>>,
    pub last_updated_ms: i64,
}

/// Columnar running-aggregate state: all keys in one IPC batch, each accumulator's
/// state across all groups in one IPC batch. Row `j` of `keys_ipc`, every
/// `acc_state_ipc[i]`, and `last_updated_ms[j]` refer to the same group.
#[derive(Clone, rkyv::Archive, rkyv::Serialize, rkyv::Deserialize)]
pub(crate) struct AggStateCheckpoint {
    pub fingerprint: u64,
    pub keys_ipc: Vec<u8>,
    pub acc_state_ipc: Vec<Vec<u8>>,
    pub last_updated_ms: Vec<i64>,
    pub last_emitted: Vec<EmittedCheckpoint>,
}

#[derive(Clone, rkyv::Archive, rkyv::Serialize, rkyv::Deserialize)]
pub(crate) struct EmittedCheckpoint {
    pub key: Vec<u8>,
    pub values: Vec<u8>,
}

/// Query-plan limits for borrowed aggregate archive validation.
#[derive(Clone, Copy)]
pub(crate) struct AggStateArchiveRestoreProfile {
    fingerprint: u64,
    accumulator_states: usize,
    max_groups: usize,
    grouped: bool,
    emit_changelog: bool,
}

/// A checked archive retained in borrowed form until the complete roster passes.
pub(crate) struct PreflightedAggStateArchive<'a> {
    archived: &'a ArchivedAggStateCheckpoint,
    group_count: usize,
}

impl AggStateArchiveRestoreProfile {
    pub(super) const fn new(
        fingerprint: u64,
        accumulator_states: usize,
        max_groups: usize,
        grouped: bool,
        emit_changelog: bool,
    ) -> Self {
        Self {
            fingerprint,
            accumulator_states,
            max_groups,
            grouped,
            emit_changelog,
        }
    }

    pub(crate) fn preflight<'a>(
        self,
        bytes: &'a [u8],
        context: std::fmt::Arguments<'_>,
    ) -> Result<PreflightedAggStateArchive<'a>, DbError> {
        let archived = rkyv::access::<ArchivedAggStateCheckpoint, rkyv::rancor::Error>(bytes)
            .map_err(|error| {
                DbError::Pipeline(format!(
                    "{context} aggregate archive validation failed: {error}"
                ))
            })?;
        if archived.fingerprint != self.fingerprint {
            return Err(DbError::Pipeline(format!(
                "{context} fingerprint mismatch: saved={}, current={}",
                archived.fingerprint, self.fingerprint
            )));
        }

        let group_count = archived.last_updated_ms.len();
        if group_count > self.max_groups {
            return Err(DbError::Pipeline(format!(
                "aggregate group limit exceeded during {context} archive preflight: archive={group_count}, limit={}",
                self.max_groups
            )));
        }
        if group_count == 0 {
            if !archived.keys_ipc.is_empty()
                || !archived.acc_state_ipc.is_empty()
                || !archived.last_emitted.is_empty()
            {
                return Err(DbError::Pipeline(format!(
                    "{context} contains a non-canonical empty aggregate checkpoint"
                )));
            }
        } else {
            if archived.acc_state_ipc.len() != self.accumulator_states {
                return Err(DbError::Pipeline(format!(
                    "{context} aggregate checkpoint has {} accumulator states; the query requires {}",
                    archived.acc_state_ipc.len(),
                    self.accumulator_states
                )));
            }
            if self.grouped && archived.keys_ipc.is_empty() {
                return Err(DbError::Pipeline(format!(
                    "{context} grouped aggregate checkpoint has {group_count} groups but no key bytes"
                )));
            }
            if !self.grouped {
                if group_count != 1 {
                    return Err(DbError::Pipeline(format!(
                        "{context} global aggregate checkpoint contains {group_count} groups"
                    )));
                }
                if !archived.keys_ipc.is_empty() {
                    return Err(DbError::Pipeline(format!(
                        "{context} global aggregate checkpoint contains key bytes"
                    )));
                }
            }
            if archived.last_emitted.len() > group_count {
                return Err(DbError::Pipeline(format!(
                    "{context} aggregate checkpoint contains {} changelog entries for {group_count} groups",
                    archived.last_emitted.len()
                )));
            }
            if !self.emit_changelog && !archived.last_emitted.is_empty() {
                return Err(DbError::Pipeline(format!(
                    "{context} aggregate checkpoint contains changelog state for a non-changelog query"
                )));
            }
        }

        Ok(PreflightedAggStateArchive {
            archived,
            group_count,
        })
    }
}

impl PreflightedAggStateArchive<'_> {
    pub(crate) const fn group_count(&self) -> usize {
        self.group_count
    }

    pub(crate) fn deserialize(
        self,
        context: std::fmt::Arguments<'_>,
    ) -> Result<AggStateCheckpoint, DbError> {
        rkyv::deserialize::<AggStateCheckpoint, rkyv::rancor::Error>(self.archived).map_err(
            |error| {
                DbError::Pipeline(format!(
                    "{context} aggregate deserialization failed: {error}"
                ))
            },
        )
    }
}

#[derive(Clone, rkyv::Archive, rkyv::Serialize, rkyv::Deserialize)]
pub(crate) struct WindowCheckpoint {
    pub window_start: i64,
    pub groups: Vec<GroupCheckpoint>,
}

/// Stable hash of the state-producing SQL and output schema; invalidates state on query change.
pub(crate) fn query_fingerprint(state_sql: &str, output_schema: &Schema) -> u64 {
    query_fingerprint_with_config(state_sql, output_schema, &[])
}

/// Query fingerprint with an operator-specific binary configuration suffix.
pub(crate) fn query_fingerprint_with_config(
    state_sql: &str,
    output_schema: &Schema,
    config: &[u8],
) -> u64 {
    let mut hasher = Xxh3::new();
    hasher.update(b"laminardb.state-query.v2\0");
    hash_bytes(&mut hasher, state_sql.as_bytes());
    hasher.update(&(output_schema.fields().len() as u64).to_le_bytes());
    for field in output_schema.fields() {
        hash_bytes(&mut hasher, field.name().as_bytes());
        hash_bytes(&mut hasher, field.data_type().to_string().as_bytes());
        hasher.update(&[u8::from(field.is_nullable())]);
    }
    hash_bytes(&mut hasher, config);
    hasher.digest()
}

fn hash_bytes(hasher: &mut Xxh3, value: &[u8]) {
    hasher.update(&(value.len() as u64).to_le_bytes());
    hasher.update(value);
}

#[cfg(all(test, feature = "cluster"))]
mod tests {
    use super::*;

    const FINGERPRINT: u64 = 7;

    fn grouped_profile() -> AggStateArchiveRestoreProfile {
        AggStateArchiveRestoreProfile::new(FINGERPRINT, 1, 1, true, false)
    }

    fn one_group() -> AggStateCheckpoint {
        AggStateCheckpoint {
            fingerprint: FINGERPRINT,
            keys_ipc: vec![1],
            acc_state_ipc: vec![Vec::new()],
            last_updated_ms: vec![i64::MIN],
            last_emitted: Vec::new(),
        }
    }

    fn encode(checkpoint: &AggStateCheckpoint) -> rkyv::util::AlignedVec<16> {
        rkyv::to_bytes::<rkyv::rancor::Error>(checkpoint).unwrap()
    }

    #[test]
    fn aggregate_archive_preflight_accepts_canonical_boundaries() {
        let encoded = encode(&one_group());
        let preflighted = grouped_profile()
            .preflight(&encoded, format_args!("test"))
            .unwrap();
        assert_eq!(preflighted.group_count(), 1);

        let empty = AggStateCheckpoint {
            fingerprint: FINGERPRINT,
            keys_ipc: Vec::new(),
            acc_state_ipc: Vec::new(),
            last_updated_ms: Vec::new(),
            last_emitted: Vec::new(),
        };
        let encoded = encode(&empty);
        assert_eq!(
            grouped_profile()
                .preflight(&encoded, format_args!("empty"))
                .unwrap()
                .group_count(),
            0
        );

        let mut global = one_group();
        global.keys_ipc.clear();
        let encoded = encode(&global);
        assert_eq!(
            AggStateArchiveRestoreProfile::new(FINGERPRINT, 1, 1, false, false)
                .preflight(&encoded, format_args!("global"))
                .unwrap()
                .group_count(),
            1
        );
    }

    #[test]
    fn aggregate_archive_preflight_rejects_shape_before_owned_decode() {
        let mut cases = Vec::new();

        let mut wrong_fingerprint = one_group();
        wrong_fingerprint.fingerprint += 1;
        cases.push((wrong_fingerprint, "fingerprint mismatch"));

        let mut wrong_accumulators = one_group();
        wrong_accumulators.acc_state_ipc.clear();
        cases.push((wrong_accumulators, "accumulator states"));

        let mut missing_keys = one_group();
        missing_keys.keys_ipc.clear();
        cases.push((missing_keys, "no key bytes"));

        let mut noncanonical_empty = one_group();
        noncanonical_empty.last_updated_ms.clear();
        cases.push((noncanonical_empty, "non-canonical empty"));

        let mut unexpected_changelog = one_group();
        unexpected_changelog.last_emitted.push(EmittedCheckpoint {
            key: vec![1],
            values: vec![1],
        });
        cases.push((unexpected_changelog, "non-changelog query"));

        let mut too_many_groups = one_group();
        too_many_groups.last_updated_ms.push(0);
        cases.push((too_many_groups, "group limit exceeded"));

        for (checkpoint, expected) in cases {
            let encoded = encode(&checkpoint);
            let error = grouped_profile()
                .preflight(&encoded, format_args!("test"))
                .err()
                .expect("the malformed archive must fail preflight");
            assert!(error.to_string().contains(expected), "{error}");
        }
    }

    #[test]
    fn aggregate_archive_preflight_rejects_invalid_global_shapes() {
        let global_profile = AggStateArchiveRestoreProfile::new(FINGERPRINT, 1, 2, false, false);

        let mut keyed_global = one_group();
        let encoded = encode(&keyed_global);
        let error = global_profile
            .preflight(&encoded, format_args!("global"))
            .err()
            .expect("global key bytes must fail preflight");
        assert!(error.to_string().contains("contains key bytes"), "{error}");

        keyed_global.keys_ipc.clear();
        keyed_global.last_updated_ms.push(0);
        let encoded = encode(&keyed_global);
        let error = global_profile
            .preflight(&encoded, format_args!("global"))
            .err()
            .expect("multiple global rows must fail preflight");
        assert!(error.to_string().contains("contains 2 groups"), "{error}");
    }
}
