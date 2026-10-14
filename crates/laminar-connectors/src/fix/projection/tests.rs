use std::collections::BTreeSet;
use std::sync::Arc;

use arrow_array::cast::AsArray;
use arrow_array::types::{Decimal128Type, Int64Type, TimestampNanosecondType, UInt64Type};
use arrow_array::Array;
use arrow_schema::{DataType, Field, Fields, Schema, SchemaRef, TimeUnit};

use super::*;
use crate::config::ConnectorConfig;
use crate::fix::engine::{GroupEntry, OwnedMessage, Section};

pub(crate) const DICTIONARY: &str = concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/tests/fixtures/fix/FIX44-synthetic.xml"
);

fn dictionary() -> Arc<DataDictionary> {
    Arc::new(DataDictionary::from_path(DICTIONARY).expect("dictionary"))
}

struct Party<'a> {
    id: &'a str,
    role: &'a str,
    subs: &'a [&'a str],
}

fn execution_report(
    seq: u64,
    order_id: &str,
    parties: &[Party<'_>],
    encoded: Option<&[u8]>,
) -> Vec<u8> {
    let mut message = OwnedMessage::new().expect("message");
    let seq = seq.to_string();
    for (tag, value) in [
        (8, "FIX.4.4"),
        (35, "8"),
        (49, "VENUE"),
        (56, "LAMINAR"),
        (34, seq.as_str()),
        (52, "20261014-12:00:00.123456789"),
    ] {
        message
            .set_field(Section::Header, tag, value.as_bytes())
            .expect("header");
    }
    for (tag, value) in [
        (37, order_id),
        (17, "EXEC-1"),
        (150, "F"),
        (39, "1"),
        (55, "ABC"),
        (54, "1"),
        (31, "100.2500"),
        (32, "10"),
        (151, "0"),
        (14, "10"),
        (6, "100.25"),
        (60, "20261014-11:59:59.000001"),
        (5001, "42"),
    ] {
        message
            .set_field(Section::Body, tag, value.as_bytes())
            .expect("body");
    }
    if let Some(encoded) = encoded {
        message
            .set_field(Section::Body, 354, encoded.len().to_string().as_bytes())
            .expect("len");
        message
            .set_field(Section::Body, 355, encoded)
            .expect("data");
    }
    for party in parties {
        let subs = party
            .subs
            .iter()
            .map(|sub| GroupEntry {
                fields: vec![(523, sub), (803, "4")],
                groups: Vec::new(),
            })
            .collect();
        let mut fields = vec![(448, party.id), (447, "D"), (452, party.role)];
        if party.subs.is_empty() {
            // A present group with zero entries, distinct from an absent group.
            fields.push((802, "0"));
        }
        let entry = GroupEntry {
            fields,
            groups: vec![(802, subs)],
        };
        message.add_group(453, &entry).expect("party");
    }
    let mut bytes = Vec::new();
    message
        .view()
        .serialize_into(&mut bytes)
        .expect("serialize");
    bytes
}

fn venue_status(seq: u64) -> Vec<u8> {
    let mut message = OwnedMessage::new().expect("message");
    let seq = seq.to_string();
    for (tag, value) in [
        (8, "FIX.4.4"),
        (35, "U1"),
        (49, "VENUE"),
        (56, "LAMINAR"),
        (34, seq.as_str()),
        (52, "20261014-12:00:00"),
    ] {
        message
            .set_field(Section::Header, tag, value.as_bytes())
            .expect("header");
    }
    message
        .set_field(Section::Body, 5003, b"7")
        .expect("status");
    let mut bytes = Vec::new();
    message
        .view()
        .serialize_into(&mut bytes)
        .expect("serialize");
    bytes
}

fn record(seq: u64, msg_type: &str, message: Vec<u8>) -> ApplicationRecord {
    ApplicationRecord {
        series: 1,
        seq,
        received_at_nanos: 1_760_443_200_123_456_789,
        poss_dup: seq == 3,
        poss_resend: false,
        msg_type: msg_type.into(),
        content_digest: 0,
        message,
    }
}

fn config(mappings: &[(&str, &str)]) -> ConnectorConfig {
    let mut config = ConnectorConfig::new("fix");
    for (key, value) in mappings {
        config.set(*key, *value);
    }
    config
}

fn messages(types: &[&str]) -> BTreeSet<String> {
    types.iter().map(|value| (*value).to_string()).collect()
}

#[allow(clippy::needless_pass_by_value)] // Call sites build the schema inline.
fn projection(schema: SchemaRef, mappings: &[(&str, &str)], types: &[&str]) -> Projection {
    let dictionary = dictionary();
    let plan = compile(&config(mappings), &schema, &dictionary, &messages(types)).expect("plan");
    Projection::new(plan, dictionary, Arc::from("FIX.4.4:LAMINAR->VENUE"), 100)
}

fn party_struct() -> Fields {
    Fields::from(vec![
        Field::new("id", DataType::Utf8, false),
        Field::new("role", DataType::Int64, true),
        Field::new(
            "subs",
            DataType::List(Arc::new(Field::new(
                "item",
                DataType::Struct(Fields::from(vec![Field::new(
                    "sub_id",
                    DataType::Utf8,
                    false,
                )])),
                false,
            ))),
            true,
        ),
    ])
}

#[test]
fn scalar_fields_metadata_and_nested_groups_project_exactly() {
    let schema = Arc::new(Schema::new(vec![
        Field::new("order_id", DataType::Utf8, false),
        Field::new("last_px", DataType::Decimal128(18, 4), true),
        Field::new(
            "transact_time",
            DataType::Timestamp(TimeUnit::Nanosecond, None),
            true,
        ),
        Field::new(
            "sending_time",
            DataType::Timestamp(TimeUnit::Nanosecond, None),
            false,
        ),
        Field::new("venue_seq", DataType::Int64, true),
        Field::new("encoded", DataType::Binary, true),
        Field::new("clordid", DataType::Utf8, true),
        Field::new(
            "parties",
            DataType::List(Arc::new(Field::new(
                "item",
                DataType::Struct(party_struct()),
                false,
            ))),
            true,
        ),
        Field::new("_fix_seq_num", DataType::UInt64, false),
        Field::new("_fix_poss_dup", DataType::Boolean, false),
        Field::new(
            "_fix_received_at",
            DataType::Timestamp(TimeUnit::Microsecond, None),
            false,
        ),
        Field::new("_fix_msg_type", DataType::Utf8, false),
    ]));
    let mut projection = projection(
        schema,
        &[
            ("column.order_id", "37"),
            ("column.last_px", "31"),
            ("column.transact_time", "60"),
            ("column.sending_time", "header.52"),
            ("column.venue_seq", "5001"),
            ("column.encoded", "355"),
            ("column.clordid", "11"),
            ("column.parties", "453"),
            ("column.parties.id", "448"),
            ("column.parties.role", "452"),
            ("column.parties.subs", "802"),
            ("column.parties.subs.sub_id", "523"),
        ],
        &["8"],
    );
    let binary = b"x\x01y\x00z";
    let parties = [
        Party {
            id: "0042",
            role: "1",
            subs: &["S1", "S2"],
        },
        Party {
            id: "B",
            role: "3",
            subs: &[],
        },
    ];
    let rows = projection
        .push(
            10,
            &record(2, "8", execution_report(2, "007", &parties, Some(binary))),
        )
        .expect("push");
    assert_eq!(rows, 1);
    projection
        .push(11, &record(3, "8", execution_report(3, "008", &[], None)))
        .expect("push");
    let (batch, positions) = projection.finish().expect("finish");

    assert_eq!(batch.num_rows(), 2);
    assert_eq!(
        batch.column(0).as_string::<i32>().value(0),
        "007",
        "leading zeros kept"
    );
    let px = batch.column(1).as_primitive::<Decimal128Type>();
    assert_eq!(px.value(0), 1_002_500);
    let transact = batch.column(2).as_primitive::<TimestampNanosecondType>();
    assert_eq!(transact.value(0) % 1_000_000_000, 1_000);
    assert_eq!(batch.column(4).as_primitive::<Int64Type>().value(0), 42);
    assert_eq!(batch.column(5).as_binary::<i32>().value(0), binary);
    assert!(batch.column(5).is_null(1));
    assert!(batch.column(6).is_null(0), "absent optional field is null");
    let parties = batch.column(7).as_list::<i32>();
    assert_eq!(parties.value_length(0), 2);
    assert!(parties.is_null(1), "absent group is null, not empty");
    let first = parties.value(0);
    let first = first.as_struct();
    assert_eq!(first.column(0).as_string::<i32>().value(0), "0042");
    let subs = first.column(2).as_list::<i32>();
    assert_eq!(subs.value_length(0), 2);
    assert_eq!(
        subs.value_length(1),
        0,
        "present group with zero entries is empty"
    );
    assert!(!subs.is_null(1));
    assert_eq!(batch.column(8).as_primitive::<UInt64Type>().value(1), 3);
    assert!(batch.column(9).as_boolean().value(1));
    assert_eq!(batch.column(11).as_string::<i32>().value(0), "8");
    assert_eq!(positions.order_key().value(0), 10_u64.to_be_bytes());
    assert_eq!(positions.sub_offset().value(1), 0);
}

#[test]
fn expansion_emits_one_row_per_entry_without_cartesian_products() {
    let schema = Arc::new(Schema::new(vec![
        Field::new("order_id", DataType::Utf8, false),
        Field::new("party_id", DataType::Utf8, true),
        Field::new(
            "party_subs",
            DataType::List(Arc::new(Field::new(
                "item",
                DataType::Struct(Fields::from(vec![Field::new(
                    "sub_id",
                    DataType::Utf8,
                    false,
                )])),
                false,
            ))),
            true,
        ),
        Field::new("_fix_row", DataType::UInt32, false),
    ]));
    let mappings = [
        ("expand", "453"),
        ("column.order_id", "37"),
        ("column.party_id", "453[].448"),
        ("column.party_subs", "453[].802"),
        ("column.party_subs.sub_id", "523"),
    ];
    let mut projection = projection(schema.clone(), &mappings, &["8"]);
    let parties = [
        Party {
            id: "A",
            role: "1",
            subs: &["S1", "S2"],
        },
        Party {
            id: "B",
            role: "3",
            subs: &["S3"],
        },
        Party {
            id: "C",
            role: "3",
            subs: &[],
        },
    ];
    assert_eq!(
        projection
            .push(5, &record(2, "8", execution_report(2, "O", &parties, None)))
            .expect("push"),
        3
    );
    assert_eq!(
        projection
            .push(6, &record(4, "8", execution_report(4, "P", &[], None)))
            .expect("push"),
        1
    );
    let (batch, positions) = projection.finish().expect("finish");
    assert_eq!(batch.num_rows(), 4);
    let ids = batch.column(1).as_string::<i32>();
    assert_eq!((ids.value(0), ids.value(1), ids.value(2)), ("A", "B", "C"));
    assert!(
        ids.is_null(3),
        "empty expansion row carries null entry values"
    );
    let subs = batch.column(2).as_list::<i32>();
    assert_eq!((subs.value_length(0), subs.value_length(1)), (2, 1));
    let rows: Vec<u32> = positions.sub_offset().values().to_vec();
    assert_eq!(rows, vec![0, 1, 2, 0]);

    let mut skip: Vec<(&str, &str)> = mappings.to_vec();
    skip.push(("expand.empty", "skip"));
    let mut projection = projection_with(&schema, &skip);
    assert_eq!(
        projection
            .push(6, &record(4, "8", execution_report(4, "P", &[], None)))
            .expect("push"),
        0
    );
}

fn projection_with(schema: &SchemaRef, mappings: &[(&str, &str)]) -> Projection {
    projection(schema.clone(), mappings, &["8"])
}

#[test]
fn group_members_project_as_aligned_scalar_lists() {
    let list = |item: DataType| DataType::List(Arc::new(Field::new("item", item, true)));
    let schema = Arc::new(Schema::new(vec![
        Field::new("party_ids", list(DataType::Utf8), true),
        Field::new("party_roles", list(DataType::Int64), true),
        Field::new("sub_ids", list(list(DataType::Utf8)), true),
    ]));
    let mut projection = projection(
        schema,
        &[
            ("column.party_ids", "453.448"),
            ("column.party_roles", "453.452"),
            ("column.sub_ids", "453.802.523"),
        ],
        &["8"],
    );
    let parties = [
        Party {
            id: "A",
            role: "1",
            subs: &["S1", "S2"],
        },
        Party {
            id: "B",
            role: "3",
            subs: &[],
        },
    ];
    projection
        .push(1, &record(2, "8", execution_report(2, "O", &parties, None)))
        .expect("push");
    let (batch, _) = projection.finish().expect("finish");
    let ids = batch.column(0).as_list::<i32>().value(0);
    let ids = ids.as_string::<i32>();
    assert_eq!((ids.value(0), ids.value(1)), ("A", "B"));
    let roles = batch.column(1).as_list::<i32>().value(0);
    assert_eq!(
        roles.as_primitive::<Int64Type>().values().to_vec(),
        vec![1, 3]
    );
    let subs = batch.column(2).as_list::<i32>().value(0);
    let subs = subs.as_list::<i32>();
    assert_eq!(
        (subs.value_length(0), subs.value_length(1)),
        (2, 0),
        "entry boundaries kept"
    );
}

#[test]
fn mixed_message_types_share_one_nullable_schema() {
    let schema = Arc::new(Schema::new(vec![
        Field::new("order_id", DataType::Utf8, true),
        Field::new("status", DataType::Int64, true),
        Field::new("_fix_msg_type", DataType::Utf8, false),
    ]));
    let mut projection = projection(
        schema,
        &[("column.order_id", "37"), ("column.status", "5003")],
        &["8", "U1"],
    );
    projection
        .push(1, &record(2, "8", execution_report(2, "O", &[], None)))
        .expect("push");
    projection
        .push(2, &record(4, "U1", venue_status(4)))
        .expect("push");
    let (batch, _) = projection.finish().expect("finish");
    assert!(batch.column(1).is_null(0));
    assert!(batch.column(0).is_null(1));
    assert_eq!(batch.column(1).as_primitive::<Int64Type>().value(1), 7);
}

#[test]
fn invalid_values_fail_explicitly_instead_of_becoming_null() {
    let rounding = Arc::new(Schema::new(vec![Field::new(
        "px",
        DataType::Decimal128(10, 1),
        true,
    )]));
    let mut projection = projection(rounding, &[("column.px", "31")], &["8"]);
    let error = projection
        .push(1, &record(2, "8", execution_report(2, "O", &[], None)))
        .expect_err("100.2500 needs scale 2; scale 1 would round");
    assert!(error.to_string().contains("px"), "{error}");
}

#[test]
fn missing_required_values_fail_with_the_message_identity() {
    let schema = Arc::new(Schema::new(vec![Field::new(
        "clordid",
        DataType::Utf8,
        false,
    )]));
    let mut projection = projection(schema, &[("column.clordid", "11")], &["8"]);
    let error = projection
        .push(9, &record(2, "8", execution_report(2, "O", &[], None)))
        .expect_err("required");
    let text = error.to_string();
    assert!(
        text.contains("seq 2") && text.contains("journal record 9") && text.contains("tag 11"),
        "{text}"
    );
}

/// A schema, its mapping options, and the expected error text.
type MappingCase<'a> = (SchemaRef, &'a [(&'a str, &'a str)], &'a str);

#[test]
fn mappings_are_validated_against_schema_and_dictionary_before_activation() {
    let dictionary = dictionary();
    let types = messages(&["8"]);
    let utf8 = |name: &str| Arc::new(Schema::new(vec![Field::new(name, DataType::Utf8, true)]));
    let cases: &[MappingCase<'_>] = &[
        (utf8("a"), &[], "no 'column.a' mapping"),
        (utf8("a"), &[("column.a", "9999")], "not in the dictionary"),
        (utf8("a"), &[("column.a", "5003")], "not a Body field"),
        (utf8("a"), &[("column.a", "448")], "not a Body field"),
        (
            utf8("a"),
            &[("column.a", "37"), ("column.b", "17")],
            "does not name a declared column",
        ),
        (
            utf8("a"),
            &[("column.a", "453[].448")],
            "'expand' is not set",
        ),
        (utf8("a"), &[("column.a", "@nope")], "unknown FIX metadata"),
        (utf8("a"), &[("column.a", "355")], "cannot be read as"),
        (
            Arc::new(Schema::new(vec![Field::new("a", DataType::Int64, true)])),
            &[("column.a", "31")],
            "cannot be read as",
        ),
        (
            Arc::new(Schema::new(vec![Field::new("a", DataType::Boolean, true)])),
            &[("column.a", "@seq_num")],
            "cannot be",
        ),
    ];
    for (schema, mappings, expected) in cases {
        let error = compile(&config(mappings), schema, &dictionary, &types).expect_err(expected);
        assert!(error.to_string().contains(expected), "{expected}: {error}");
    }
    assert!(compile(
        &config(&[("column.a", "37")]),
        &utf8("a"),
        &dictionary,
        &messages(&["ZZ"])
    )
    .is_err());
}

/// Projection CPU cost per journaled message (engine parse plus Arrow building).
#[test]
#[ignore = "performance profile; run in release with --ignored"]
fn profile_projection_throughput() {
    let schema = Arc::new(Schema::new(vec![
        Field::new("order_id", DataType::Utf8, false),
        Field::new("last_px", DataType::Decimal128(18, 4), true),
        Field::new(
            "transact_time",
            DataType::Timestamp(TimeUnit::Nanosecond, None),
            true,
        ),
        Field::new("venue_seq", DataType::Int64, true),
        Field::new("party_id", DataType::Utf8, true),
        Field::new("_fix_seq_num", DataType::UInt64, false),
    ]));
    let mappings = [
        ("expand", "453"),
        ("column.order_id", "37"),
        ("column.last_px", "31"),
        ("column.transact_time", "60"),
        ("column.venue_seq", "5001"),
        ("column.party_id", "453[].448"),
    ];
    for entries in [0_usize, 3, 10] {
        let ids: Vec<String> = (0..entries).map(|index| format!("P{index}")).collect();
        let parties: Vec<Party<'_>> = ids
            .iter()
            .map(|id| Party {
                id,
                role: "1",
                subs: &[],
            })
            .collect();
        let message = record(2, "8", execution_report(2, "ORD", &parties, None));
        let mut projection = projection(schema.clone(), &mappings, &["8"]);
        let messages = 20_000_u64;
        let begin = std::time::Instant::now();
        let mut rows = 0;
        for index in 0..messages {
            rows += projection.push(index, &message).expect("push");
            if projection.rows() >= 1024 {
                projection.finish().expect("finish");
            }
        }
        projection.finish().expect("finish");
        let elapsed = begin.elapsed();
        eprintln!(
            "projection entries={entries:>2} bytes={} messages/s={:.0} rows/s={:.0} per-message={:?}",
            message.message.len(),
            messages as f64 / elapsed.as_secs_f64(),
            rows as f64 / elapsed.as_secs_f64(),
            elapsed / u32::try_from(messages).expect("count")
        );
    }
}
