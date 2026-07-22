use super::*;

const TEST_VNODE: u32 = 39;

fn digest(byte: u8) -> [u8; SHA256_LEN] {
    [byte; SHA256_LEN]
}

fn roster_entry(operator: u8, table: u8) -> ExpectedRosterEntry {
    ExpectedRosterEntry {
        operator_identity_sha256: digest(operator),
        state_table_identity_sha256: digest(table),
        vnode: TEST_VNODE,
        managed_envelope_version: MANAGED_ENVELOPE_VERSION,
    }
}

fn context(roster: &[ExpectedRosterEntry]) -> ExpectedContext<'_> {
    ExpectedContext {
        attempt: CheckpointAttempt::canonical(10),
        assignment_version: 7,
        partitioning_abi_version: PARTITIONING_ABI_VERSION,
        vnode_count: 257,
        vnode: TEST_VNODE,
        assignment_certificate_sha256: digest(0xa5),
        roster,
    }
}

const fn limits() -> VnodePartialV2Limits {
    VnodePartialV2Limits {
        encoded_artifact_bytes_max: 4096,
        envelope_metadata_bytes_max: 2048,
        directory_entries_per_artifact_max: 4,
    }
}

fn full_entry(operator: u8, table: u8, body: &'static [u8]) -> EncodeEntry<'static> {
    EncodeEntry {
        operator_identity_sha256: digest(operator),
        state_table_identity_sha256: digest(table),
        payload: EncodeEntryPayload::Body {
            artifact_kind: ArtifactKind::Full,
            body,
            parent: None,
        },
    }
}

fn parent(checkpoint_id: u64, byte: u8) -> ParentEntryLink {
    ParentEntryLink {
        attempt: CheckpointAttempt::canonical(checkpoint_id),
        entry_sha256: digest(byte),
    }
}

fn put_test(bytes: &mut [u8], offset: usize, value: &[u8]) {
    put(bytes, offset, value).unwrap();
}

fn rehash_directory(bytes: &mut [u8]) {
    let directory_offset = usize::try_from(read_u64(bytes, 64).unwrap()).unwrap();
    let directory_len = usize::try_from(read_u64(bytes, 72).unwrap()).unwrap();
    let directory_end = directory_offset + directory_len;
    let digest = sha256(&bytes[directory_offset..directory_end]);
    put_test(bytes, 128, &digest);
}

fn fixture_bytes(text: &str) -> Vec<u8> {
    let compact = text
        .bytes()
        .filter(|byte| !byte.is_ascii_whitespace())
        .collect::<Vec<_>>();
    let mut chunks = compact.chunks_exact(2);
    let bytes = chunks
        .by_ref()
        .map(|pair| {
            u8::from_str_radix(std::str::from_utf8(pair).unwrap(), 16)
                .expect("fixture contains only hexadecimal bytes")
        })
        .collect();
    assert!(chunks.remainder().is_empty(), "fixture has an odd nibble");
    bytes
}

#[test]
fn frozen_v2_outer_directory_goldens_decode_and_encoder_reproduces_them() {
    let reference_roster = [roster_entry(1, 11)];
    let reference = [EncodeEntry {
        operator_identity_sha256: digest(1),
        state_table_identity_sha256: digest(11),
        payload: EncodeEntryPayload::Reference {
            parent: parent(4, 0x44),
        },
    }];
    let reference_fixture = fixture_bytes(include_str!(
        "../../../tests/fixtures/managed_state_v1/vnode_partial_reference.hex"
    ));
    let reference_encoded = encode(context(&reference_roster), &reference, limits()).unwrap();
    assert_eq!(reference_encoded, reference_fixture);
    let decoded_reference =
        decode(&reference_fixture, context(&reference_roster), limits()).unwrap();
    assert!(matches!(
        decoded_reference.entries().next().unwrap().unwrap().payload,
        DecodedEntryPayload::Reference { parent: link }
            if link == parent(4, 0x44)
    ));

    let mixed_roster = [
        roster_entry(1, 11),
        roster_entry(2, 12),
        roster_entry(3, 13),
    ];
    // These placeholder BODY bytes deliberately exercise only the outer directory layer. A
    // composed golden separately validates real managed-envelope BODY bytes.
    let mixed = [
        full_entry(1, 11, b"full-envelope"),
        EncodeEntry {
            operator_identity_sha256: digest(2),
            state_table_identity_sha256: digest(12),
            payload: EncodeEntryPayload::Body {
                artifact_kind: ArtifactKind::Empty,
                body: b"empty-envelope",
                parent: None,
            },
        },
        EncodeEntry {
            operator_identity_sha256: digest(3),
            state_table_identity_sha256: digest(13),
            payload: EncodeEntryPayload::Reference {
                parent: parent(4, 0x44),
            },
        },
    ];
    let mixed_fixture = fixture_bytes(include_str!(
        "../../../tests/fixtures/managed_state_v1/vnode_partial_mixed.hex"
    ));
    let mixed_encoded = encode(context(&mixed_roster), &mixed, limits()).unwrap();
    assert_eq!(mixed_encoded, mixed_fixture);
    let decoded_mixed = decode(&mixed_fixture, context(&mixed_roster), limits()).unwrap();
    let payloads = decoded_mixed
        .entries()
        .map(|entry| entry.unwrap().payload)
        .collect::<Vec<_>>();
    assert!(matches!(
        payloads[0],
        DecodedEntryPayload::Body {
            artifact_kind: ArtifactKind::Full,
            body: b"full-envelope",
            parent: None,
            ..
        }
    ));
    assert!(matches!(
        payloads[1],
        DecodedEntryPayload::Body {
            artifact_kind: ArtifactKind::Empty,
            body: b"empty-envelope",
            parent: None,
            ..
        }
    ));
    assert!(matches!(payloads[2], DecodedEntryPayload::Reference { .. }));
}

#[test]
fn outer_mixed_directory_round_trips_as_borrowed_views() {
    let roster = [
        roster_entry(1, 11),
        roster_entry(2, 12),
        roster_entry(3, 13),
    ];
    let entries = [
        full_entry(1, 11, b"full-envelope"),
        EncodeEntry {
            operator_identity_sha256: digest(2),
            state_table_identity_sha256: digest(12),
            payload: EncodeEntryPayload::Body {
                artifact_kind: ArtifactKind::Empty,
                body: b"empty-envelope",
                parent: None,
            },
        },
        EncodeEntry {
            operator_identity_sha256: digest(3),
            state_table_identity_sha256: digest(13),
            payload: EncodeEntryPayload::Reference {
                parent: parent(4, 0x44),
            },
        },
    ];

    let encoded = encode(context(&roster), &entries, limits()).unwrap();
    let decoded = decode(&encoded, context(&roster), limits()).unwrap();
    assert_eq!(decoded.attempt(), CheckpointAttempt::canonical(10));
    assert_eq!(decoded.assignment_version(), 7);
    assert_eq!(decoded.partitioning_abi_version(), PARTITIONING_ABI_VERSION);
    assert_eq!(decoded.vnode_count(), 257);
    assert_eq!(decoded.vnode(), TEST_VNODE);
    assert_eq!(decoded.assignment_certificate_sha256(), digest(0xa5));
    assert_eq!(decoded.entry_count(), 3);

    let mut decoded_entries = decoded.entries();
    assert_eq!(decoded_entries.len(), 3);
    let first = decoded_entries.next().unwrap().unwrap();
    assert_eq!(first.operator_identity_sha256, digest(1));
    assert_eq!(first.state_table_identity_sha256, digest(11));
    assert_eq!(first.vnode, TEST_VNODE);
    assert_eq!(first.managed_envelope_version, MANAGED_ENVELOPE_VERSION);
    assert_ne!(first.contextual_sha256, ZERO_SHA256);
    assert!(matches!(
        first.payload,
        DecodedEntryPayload::Body {
            artifact_kind: ArtifactKind::Full,
            body: b"full-envelope",
            parent: None,
            ..
        }
    ));

    let second = decoded_entries.next().unwrap().unwrap();
    assert!(matches!(
        second.payload,
        DecodedEntryPayload::Body {
            artifact_kind: ArtifactKind::Empty,
            body: b"empty-envelope",
            parent: None,
            ..
        }
    ));
    let third = decoded_entries.next().unwrap().unwrap();
    assert_eq!(
        third.payload,
        DecodedEntryPayload::Reference {
            parent: parent(4, 0x44)
        }
    );
    assert!(decoded_entries.next().is_none());
}

#[test]
fn all_reference_directory_has_canonical_empty_body() {
    let roster = [roster_entry(1, 11)];
    let entries = [EncodeEntry {
        operator_identity_sha256: digest(1),
        state_table_identity_sha256: digest(11),
        payload: EncodeEntryPayload::Reference {
            parent: parent(2, 0x22),
        },
    }];
    let encoded = encode(context(&roster), &entries, limits()).unwrap();

    assert_eq!(read_u64(&encoded, 88).unwrap(), 0);
    let decoded = decode(&encoded, context(&roster), limits()).unwrap();
    assert!(matches!(
        decoded.entries().next().unwrap().unwrap().payload,
        DecodedEntryPayload::Reference { .. }
    ));
}

#[test]
fn delta_is_immediate_but_reference_may_skip_attempts() {
    let roster = [roster_entry(1, 11)];
    let delta = [EncodeEntry {
        operator_identity_sha256: digest(1),
        state_table_identity_sha256: digest(11),
        payload: EncodeEntryPayload::Body {
            artifact_kind: ArtifactKind::Delta,
            body: b"delta-envelope",
            parent: Some(parent(9, 0x99)),
        },
    }];
    assert!(encode(context(&roster), &delta, limits()).is_ok());

    let skipped_delta = [EncodeEntry {
        payload: EncodeEntryPayload::Body {
            artifact_kind: ArtifactKind::Delta,
            body: b"delta-envelope",
            parent: Some(parent(8, 0x88)),
        },
        ..delta[0]
    }];
    assert!(encode(context(&roster), &skipped_delta, limits()).is_err());

    let skipped_reference = [EncodeEntry {
        operator_identity_sha256: digest(1),
        state_table_identity_sha256: digest(11),
        payload: EncodeEntryPayload::Reference {
            parent: parent(2, 0x22),
        },
    }];
    assert!(encode(context(&roster), &skipped_reference, limits()).is_ok());
}

#[test]
fn contextual_entry_digest_binds_containing_checkpoint_and_assignment() {
    let roster = [roster_entry(1, 11)];
    let entries = [full_entry(1, 11, b"body")];
    let encoded = encode(context(&roster), &entries, limits()).unwrap();
    let decoded = decode(&encoded, context(&roster), limits()).unwrap();
    let entry = decoded.entries().next().unwrap().unwrap();

    let raw = &encoded[HEADER_LEN..HEADER_LEN + ENTRY_LEN];
    assert_eq!(
        entry.contextual_sha256,
        contextual_entry_sha256(digest_context(context(&roster)), raw).unwrap()
    );
    let mut changed = digest_context(context(&roster));
    changed.assignment_version += 1;
    assert_ne!(
        entry.contextual_sha256,
        contextual_entry_sha256(changed, raw).unwrap()
    );
}

#[test]
fn truncation_at_every_offset_and_trailing_bytes_fail_closed() {
    let roster = [roster_entry(1, 11)];
    let entries = [full_entry(1, 11, b"body")];
    let encoded = encode(context(&roster), &entries, limits()).unwrap();

    for end in 0..encoded.len() {
        assert!(
            decode(&encoded[..end], context(&roster), limits()).is_err(),
            "truncation at {end} was accepted"
        );
    }
    let mut trailing = encoded;
    trailing.push(0);
    assert!(decode(&trailing, context(&roster), limits()).is_err());
}

#[test]
fn hostile_header_and_directory_shapes_fail_after_digest_repair() {
    let roster = [roster_entry(1, 11)];
    let entries = [full_entry(1, 11, b"body")];
    let valid = encode(context(&roster), &entries, limits()).unwrap();

    let mut reserved = valid.clone();
    put_test(&mut reserved, 14, &1_u16.to_be_bytes());
    assert!(decode(&reserved, context(&roster), limits()).is_err());

    let mut unknown_kind = valid.clone();
    unknown_kind[HEADER_LEN + 68] = 99;
    rehash_directory(&mut unknown_kind);
    assert!(decode(&unknown_kind, context(&roster), limits()).is_err());

    let mut gap = valid.clone();
    let offset = read_u64(&gap, HEADER_LEN + 72).unwrap() + 1;
    put_test(&mut gap, HEADER_LEN + 72, &offset.to_be_bytes());
    rehash_directory(&mut gap);
    assert!(decode(&gap, context(&roster), limits()).is_err());

    let mut forged_entry_digest = valid.clone();
    forged_entry_digest[HEADER_LEN + 88] ^= 1;
    rehash_directory(&mut forged_entry_digest);
    assert!(decode(&forged_entry_digest, context(&roster), limits()).is_err());

    let mut forged_body = valid;
    let body_offset = usize::try_from(read_u64(&forged_body, 80).unwrap()).unwrap();
    forged_body[body_offset] ^= 1;
    assert!(decode(&forged_body, context(&roster), limits()).is_err());
}

#[test]
fn reference_body_fields_and_noncanonical_parents_are_rejected() {
    let roster = [roster_entry(1, 11)];
    let entries = [EncodeEntry {
        operator_identity_sha256: digest(1),
        state_table_identity_sha256: digest(11),
        payload: EncodeEntryPayload::Reference {
            parent: parent(2, 0x22),
        },
    }];
    let valid = encode(context(&roster), &entries, limits()).unwrap();

    let mut body_offset = valid.clone();
    put_test(&mut body_offset, HEADER_LEN + 72, &1_u64.to_be_bytes());
    rehash_directory(&mut body_offset);
    assert!(decode(&body_offset, context(&roster), limits()).is_err());

    let current_parent = [EncodeEntry {
        payload: EncodeEntryPayload::Reference {
            parent: parent(10, 0x10),
        },
        ..entries[0]
    }];
    assert!(encode(context(&roster), &current_parent, limits()).is_err());

    let zero_digest_parent = [EncodeEntry {
        payload: EncodeEntryPayload::Reference {
            parent: ParentEntryLink {
                attempt: CheckpointAttempt::canonical(2),
                entry_sha256: ZERO_SHA256,
            },
        },
        ..entries[0]
    }];
    assert!(encode(context(&roster), &zero_digest_parent, limits()).is_err());
}

#[test]
fn exact_roster_and_injected_limits_are_enforced_before_acceptance() {
    let roster = [roster_entry(1, 11), roster_entry(2, 12)];
    let entries = [full_entry(1, 11, b"one"), full_entry(2, 12, b"two")];
    let encoded = encode(context(&roster), &entries, limits()).unwrap();

    let short_roster = [roster_entry(1, 11)];
    assert!(decode(&encoded, context(&short_roster), limits()).is_err());

    let one_entry_limit = VnodePartialV2Limits {
        directory_entries_per_artifact_max: 1,
        ..limits()
    };
    assert!(decode(&encoded, context(&roster), one_entry_limit).is_err());
    assert!(encode(context(&roster), &entries, one_entry_limit).is_err());

    let exact_bytes = VnodePartialV2Limits {
        encoded_artifact_bytes_max: u64::try_from(encoded.len()).unwrap(),
        ..limits()
    };
    assert!(decode(&encoded, context(&roster), exact_bytes).is_ok());
    let one_byte_short = VnodePartialV2Limits {
        encoded_artifact_bytes_max: exact_bytes.encoded_artifact_bytes_max - 1,
        ..exact_bytes
    };
    assert!(decode(&encoded, context(&roster), one_byte_short).is_err());
    assert!(encode(context(&roster), &entries, one_byte_short).is_err());

    let exact_metadata = VnodePartialV2Limits {
        envelope_metadata_bytes_max: u64::try_from(HEADER_LEN + 2 * ENTRY_LEN).unwrap(),
        ..limits()
    };
    assert!(decode(&encoded, context(&roster), exact_metadata).is_ok());
    assert!(encode(context(&roster), &entries, exact_metadata).is_ok());
    let one_metadata_byte_short = VnodePartialV2Limits {
        envelope_metadata_bytes_max: exact_metadata.envelope_metadata_bytes_max - 1,
        ..limits()
    };
    assert!(decode(&encoded, context(&roster), one_metadata_byte_short).is_err());
    assert!(encode(context(&roster), &entries, one_metadata_byte_short).is_err());
}

#[test]
fn unsorted_or_duplicate_rosters_and_encoder_entries_are_rejected() {
    let unsorted = [roster_entry(2, 12), roster_entry(1, 11)];
    let entries = [full_entry(2, 12, b"two"), full_entry(1, 11, b"one")];
    assert!(encode(context(&unsorted), &entries, limits()).is_err());

    let duplicate = [roster_entry(1, 11), roster_entry(1, 11)];
    let duplicate_entries = [full_entry(1, 11, b"one"), full_entry(1, 11, b"two")];
    assert!(encode(context(&duplicate), &duplicate_entries, limits()).is_err());

    let sorted = [roster_entry(1, 11), roster_entry(2, 12)];
    assert!(encode(context(&sorted), &entries, limits()).is_err());
}

#[test]
fn recomputed_internal_digests_do_not_override_expected_provenance() {
    let roster = [roster_entry(1, 11)];
    let entries = [full_entry(1, 11, b"body")];
    let mut encoded = encode(context(&roster), &entries, limits()).unwrap();
    put_test(&mut encoded, 40, &8_u64.to_be_bytes());

    assert!(decode(&encoded, context(&roster), limits()).is_err());
}

proptest::proptest! {
    #![proptest_config(proptest::test_runner::Config::with_cases(96))]

    #[test]
    fn arbitrary_bounded_input_never_panics(bytes in proptest::collection::vec(proptest::num::u8::ANY, 0..1024)) {
        let roster = [roster_entry(1, 11)];
        let _ = decode(&bytes, context(&roster), limits());
    }
}
