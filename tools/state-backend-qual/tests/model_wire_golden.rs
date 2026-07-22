use sha2::{Digest, Sha256};

use state_backend_qual::model::{
    encode_observation, encode_request, BatchKind, BatchLimits, LogicalBatch, LogicalKey, Mutation,
    RangeRead, ReferenceModel, RestoreBudget, Scenario, Table,
};

fn key(table: Table, vnode: u32, bytes: &[u8]) -> LogicalKey {
    LogicalKey {
        table,
        vnode,
        key: bytes.to_vec(),
    }
}

fn decode_hex(value: &str) -> Vec<u8> {
    assert!(value.len().is_multiple_of(2));
    value
        .as_bytes()
        .chunks_exact(2)
        .map(|pair| {
            let digit = |byte: u8| match byte {
                b'0'..=b'9' => byte - b'0',
                b'a'..=b'f' => byte - b'a' + 10,
                _ => panic!("golden contains non-lowercase-hex byte"),
            };
            (digit(pair[0]) << 4) | digit(pair[1])
        })
        .collect()
}

fn digest(value: &[u8]) -> Vec<u8> {
    Sha256::digest(value).to_vec()
}

fn stream_digest(domain: &[u8], items: &[&[u8]]) -> Vec<u8> {
    let mut hasher = Sha256::new();
    hasher.update(domain);
    hasher.update(u64::try_from(items.len()).unwrap().to_be_bytes());
    for item in items {
        hasher.update(u64::try_from(item.len()).unwrap().to_be_bytes());
        hasher.update(item);
    }
    hasher.finalize().to_vec()
}

fn trace_digest(request: &[u8], observation: &[u8]) -> Vec<u8> {
    let mut hasher = Sha256::new();
    hasher.update(b"LDB-SBQ-TRACE-V1\0");
    hasher.update(1_u64.to_be_bytes());
    for item in [request, observation] {
        hasher.update(u64::try_from(item.len()).unwrap().to_be_bytes());
        hasher.update(item);
    }
    hasher.finalize().to_vec()
}

#[test]
fn hand_authored_v1_wire_and_digest_golden() {
    let aggregate = key(Table::AggregateState, 2, &[0x00, 0xff]);
    let timer = key(Table::TimerIndex, 1, &[0x00]);
    let mut model = ReferenceModel::new(4, 8, 8).unwrap();
    let budget = RestoreBudget {
        records_max_u64: 1,
        key_bytes_max_u64: 2,
        value_bytes_max_u64: 1,
        canonical_bytes_max_u64: 16,
    };
    model
        .restore_vnode(2, &[(aggregate.clone(), Vec::new())], budget)
        .unwrap();
    model
        .restore_vnode(1, &[(timer, vec![0xaa])], budget)
        .unwrap();

    let request = LogicalBatch {
        kind: BatchKind::Measured,
        scenario: Scenario::Aggregate,
        ordinal: 0,
        logical_rows: 1,
        limits: BatchLimits {
            request_bytes_max_u64: 149,
            read_rows_max_u64: 3,
            read_bytes_max_u64: 4,
            mutation_bytes_max_u64: 1,
        },
        point_reads: vec![aggregate, key(Table::WindowState, 3, &[])],
        ranges: vec![RangeRead {
            table: Table::TimerIndex,
            vnode: 1,
            start_inclusive: vec![0x00],
            end_exclusive: vec![0x01],
            max_rows: 1,
            max_bytes: 2,
        }],
        mutations: vec![
            Mutation::Delete {
                key: key(Table::JoinLeftRows, 0, &[0x7f]),
            },
            Mutation::Put {
                key: key(Table::OutputBookkeeping, 3, &[]),
                value: Vec::new(),
            },
        ],
    };

    let expected_request = decode_hex(concat!(
        "4c44422d5342512d524551554553542d5631000101000000000000000000000001",
        "0000000000000095000000000000000300000000000000040000000000000001",
        "0000000201000000020000000200ff020000000300000000000000010300000001",
        "000000010000000001010000000100000000000000020000000202040000000000",
        "0000017f0106000000030000000000000000"
    ));
    let request_bytes = encode_request(&request).unwrap();
    assert_eq!(request_bytes, expected_request);
    assert_eq!(
        digest(&request_bytes),
        decode_hex("123ed9013922ea3c89c11d4a3eadfb24a98b510aaf0ab152ed5529f52721f0a4")
    );

    let observation = model.execute(&request).unwrap();
    let expected_observation = decode_hex(concat!(
        "4c44422d5342512d4f42534552564154494f4e2d56310001010000000000000000",
        "0000000201000000020000000200ff010000000002000000030000000000000000",
        "010300000001000000010000000001010000000100000000000000020000000103",
        "00000001000000010000000001aa00"
    ));
    let observation_bytes = encode_observation(&observation).unwrap();
    assert_eq!(observation_bytes, expected_observation);
    assert_eq!(
        digest(&observation_bytes),
        decode_hex("a76c83d79219e9ab92991f18f9b597122cf51c1a0083957aab4ebaf67f652174")
    );

    let state_input = decode_hex(concat!(
        "4c44422d5342512d53544154452d56310000000000000000030100000002000000",
        "0200ff000000000300000001000000010000000001aa0600000003000000000000",
        "0000"
    ));
    let expected_state =
        decode_hex("e570b99a8ac60c221ced6b130f0bd21f3144845ba70004893f33068b901b2c75");
    assert_eq!(digest(&state_input), expected_state);
    assert_eq!(model.live_digest().unwrap().to_vec(), expected_state);

    assert_eq!(
        stream_digest(b"LDB-SBQ-REQUEST-STREAM-V1\0", &[&request_bytes]),
        decode_hex("06f9ae77b4c73f14d1ec8e2b30d377295b59b2d3894233cb81c188e95fd87011")
    );
    assert_eq!(
        stream_digest(b"LDB-SBQ-OBSERVATION-STREAM-V1\0", &[&observation_bytes]),
        decode_hex("43c91d38aa83b50ab2583945a1cef303e7235eb3d1629bb1fa8e4d5aeb23382f")
    );
    assert_eq!(
        trace_digest(&request_bytes, &observation_bytes),
        decode_hex("51834461008758fe436b47e19ce534a68f882c8753af601eef776547edd02efa")
    );
}
