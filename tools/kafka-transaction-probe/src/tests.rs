use super::*;

#[test]
fn cli_is_manual_and_fail_closed() {
    let valid = parse_cli(
        ["--brokers", "127.0.0.1:19092", "--run-id", "run_1"]
            .into_iter()
            .map(str::to_owned),
    )
    .unwrap();
    match valid {
        Command::Run(cli) => assert_eq!(
            cli,
            Cli {
                brokers: "127.0.0.1:19092".to_owned(),
                run_id: Some("run_1".to_owned())
            }
        ),
        Command::RunAmbiguity(_) => panic!("valid deterministic arguments returned ambiguity"),
        Command::Help => panic!("valid arguments returned help"),
    }
    for hostile in [
        vec![],
        vec!["--unknown", "x"],
        vec!["--brokers"],
        vec!["--brokers", "x", "--brokers", "y"],
        vec!["--brokers=x"],
        vec!["--brokers", " x"],
        vec!["--brokers", "x", "--run-id", "bad/id"],
    ] {
        assert!(parse_cli(hostile.into_iter().map(str::to_owned)).is_err());
    }
}

#[test]
fn ambiguity_cli_is_explicit_loopback_and_fail_closed() {
    let command = parse_cli(
        [
            "--brokers",
            "127.0.0.1:19192",
            "--run-id",
            "cycle56-a",
            "--ambiguity",
            "marker-applied",
            "--proxy-upstream",
            "127.0.0.1:19194",
        ]
        .into_iter()
        .map(str::to_owned),
    )
    .unwrap();
    match command {
        Command::RunAmbiguity(cli) => {
            assert_eq!(cli.proxy_listen, "127.0.0.1:19192".parse().unwrap());
            assert_eq!(cli.proxy_upstream, "127.0.0.1:19194".parse().unwrap());
            assert_eq!(cli.run_id, "cycle56-a");
            assert_eq!(
                cli.scenario,
                AmbiguityScenario {
                    kind: AmbiguityKind::Marker,
                    outcome: AmbiguityOutcome::Applied,
                }
            );
        }
        Command::Run(_) | Command::Help => panic!("valid ambiguity arguments misclassified"),
    }

    for hostile in [
        vec![
            "--brokers",
            "127.0.0.1:19192",
            "--run-id",
            "x",
            "--ambiguity",
            "marker-applied",
        ],
        vec![
            "--brokers",
            "127.0.0.1:19192",
            "--proxy-upstream",
            "127.0.0.1:19194",
        ],
        vec![
            "--brokers",
            "0.0.0.0:19192",
            "--run-id",
            "x",
            "--ambiguity",
            "data-unapplied",
            "--proxy-upstream",
            "127.0.0.1:19194",
        ],
        vec![
            "--brokers",
            "127.0.0.1:19192,127.0.0.1:19193",
            "--run-id",
            "x",
            "--ambiguity",
            "data-applied",
            "--proxy-upstream",
            "127.0.0.1:19194",
        ],
        vec![
            "--brokers",
            "127.0.0.1:19192",
            "--run-id",
            "x",
            "--ambiguity",
            "marker-applied",
            "--proxy-upstream",
            "127.0.0.1:19192",
        ],
        vec![
            "--brokers",
            "127.0.0.1:19192",
            "--run-id",
            "x",
            "--ambiguity",
            "unknown",
            "--proxy-upstream",
            "127.0.0.1:19194",
        ],
    ] {
        assert!(parse_cli(hostile.into_iter().map(str::to_owned)).is_err());
    }
}

#[test]
fn transactional_id_is_stable_bounded_and_axis_sensitive() {
    let base = derive_transactional_id(&DEPLOYMENT, &INCARNATION, "sink", "shard").unwrap();
    assert_eq!(
        base,
        "ldb.tx.v1.c49ace6d02eb21ec7a2dc4424d8c3b9680fc3cd828cd754fec079b800a37411a"
    );
    assert_eq!(base.len(), 74);
    assert!(base.starts_with(TX_ID_PREFIX));
    assert!(base[TX_ID_PREFIX.len()..]
        .bytes()
        .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte)));

    let mut deployment = DEPLOYMENT;
    deployment[0] ^= 1;
    let mut incarnation = INCARNATION;
    incarnation[0] ^= 1;
    for changed in [
        derive_transactional_id(&deployment, &INCARNATION, "sink", "shard").unwrap(),
        derive_transactional_id(&DEPLOYMENT, &incarnation, "sink", "shard").unwrap(),
        derive_transactional_id(&DEPLOYMENT, &INCARNATION, "sink-2", "shard").unwrap(),
        derive_transactional_id(&DEPLOYMENT, &INCARNATION, "sink", "shard-2").unwrap(),
    ] {
        assert_ne!(changed, base);
    }
    assert_eq!(
        derive_transactional_id(&DEPLOYMENT, &INCARNATION, "sink", "shard").unwrap(),
        base
    );
    assert!(derive_transactional_id(&[0; 16], &INCARNATION, "sink", "shard").is_err());
    assert!(derive_transactional_id(&DEPLOYMENT, &[0; 16], "sink", "shard").is_err());
    assert!(derive_transactional_id(&DEPLOYMENT, &INCARNATION, "", "shard").is_err());
    assert!(derive_transactional_id(&DEPLOYMENT, &INCARNATION, "sink", "").is_err());
    assert!(derive_transactional_id(&DEPLOYMENT, &INCARNATION, "a\0b", "shard").is_err());
    assert!(derive_transactional_id(&DEPLOYMENT, &INCARNATION, "sink", "a\0b").is_err());
    assert!(derive_transactional_id(&DEPLOYMENT, &INCARNATION, "sink", &"h".repeat(65)).is_err());
    assert_ne!(
        derive_transactional_id(&DEPLOYMENT, &INCARNATION, "é", "shard").unwrap(),
        derive_transactional_id(&DEPLOYMENT, &INCARNATION, "e\u{301}", "shard").unwrap()
    );
}

#[test]
fn independently_encoded_data_matches_frozen_golden() {
    let encoded = encode_data_header(&[0xa1; 32], &TEST_INTERVAL, 0x0102_0304_0506_0708).unwrap();
    assert_eq!(encoded.as_slice(), decode_hex(DATA_GOLDEN_HEX).unwrap());
    assert!(encode_data_header(&[0; 32], &TEST_INTERVAL, 0).is_err());
    assert!(encode_data_header(&[1; 32], &[0; 16], 0).is_err());
}

#[test]
fn frozen_markers_are_exact_and_linked() {
    let first = decode_and_validate_marker(FIRST_MARKER_GOLDEN_HEX, false).unwrap();
    let successor = decode_and_validate_marker(SUCCESSOR_MARKER_GOLDEN_HEX, true).unwrap();
    assert_eq!(first.len(), 325);
    assert_eq!(successor.len(), 325);
    assert_ne!(first, successor);
    assert_eq!(&first[10..26], &FIRST_INTERVAL);
    assert_eq!(&successor[10..26], &SUCCESSOR_INTERVAL);
    assert_eq!(&successor[26..42], &FIRST_INTERVAL);
}

#[test]
fn expected_visibility_excludes_both_aborted_transactions() {
    let first = decode_and_validate_marker(FIRST_MARKER_GOLDEN_HEX, false).unwrap();
    let successor = decode_and_validate_marker(SUCCESSOR_MARKER_GOLDEN_HEX, true).unwrap();
    let uncommitted = expected_records(&first, &successor, true).unwrap();
    let committed = expected_records(&first, &successor, false).unwrap();
    assert_eq!(uncommitted.each_ref().map(Vec::len), [7, 5, 5]);
    assert_eq!(committed.each_ref().map(Vec::len), [5, 3, 3]);

    assert_eq!(uncommitted[0][0], committed[0][0]);
    assert_eq!(uncommitted[0][1], committed[0][1]);
    assert_eq!(uncommitted[0][2], uncommitted[0][3]);
    assert_eq!(uncommitted[0][3], committed[0][2]);
    assert_eq!(uncommitted[0][5], committed[0][3]);
    assert_eq!(uncommitted[0][6], committed[0][4]);
    assert_eq!(uncommitted[0][1].key, uncommitted[0][6].key);
    assert_eq!(uncommitted[0][1].payload, uncommitted[0][6].payload);
    assert_ne!(uncommitted[0][1].header, uncommitted[0][6].header);

    for index in 1..3 {
        assert_eq!(uncommitted[index][0], committed[index][0]);
        assert_eq!(uncommitted[index][1], uncommitted[index][2]);
        assert_eq!(uncommitted[index][2], committed[index][1]);
        assert_eq!(uncommitted[index][4], committed[index][2]);
    }
    for partition in &uncommitted {
        for record in partition {
            if record.key.is_none() {
                assert!(record.other_headers.is_empty());
            } else {
                assert_eq!(record.other_headers, trace_header());
            }
        }
    }
}

#[test]
fn topic_and_identity_boundaries_reject_hostile_values() {
    assert!(validate_run_id(&"a".repeat(48)).is_ok());
    assert!(validate_run_id(&"a".repeat(49)).is_err());
    assert!(validate_run_id("contains space").is_err());
    assert!(
        derive_transactional_id(&DEPLOYMENT, &INCARNATION, &"s".repeat(128), &"h".repeat(64))
            .is_ok()
    );
    assert!(derive_transactional_id(&DEPLOYMENT, &INCARNATION, &"s".repeat(129), "shard").is_err());
}
