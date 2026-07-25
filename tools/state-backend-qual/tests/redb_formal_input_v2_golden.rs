use sha2::{Digest as _, Sha256};
use state_backend_qual::redb_prescreen::{
    validate_redb_prescreen_pre_run_content, RedbPrescreenAuthorization,
};

const POLICY: &[u8] = include_bytes!("fixtures/redb-prescreen-successor-v1/policy.json");
const PAYLOAD: &[u8] = include_bytes!("fixtures/redb-prescreen-successor-v2/payload.json");
const RECEIPT: &[u8] = include_bytes!("fixtures/redb-prescreen-successor-v2/receipt.json");

fn sha256_hex(bytes: &[u8]) -> String {
    format!("{:x}", Sha256::digest(bytes))
}

#[test]
fn hand_authored_v2_bytes_are_stable_and_always_ineligible() {
    assert_eq!(POLICY.len(), 1_213);
    assert_eq!(
        sha256_hex(POLICY),
        "f016a6ba11bf05ab88450be76a547fd102d1c628af252520481efad829af98b7"
    );
    assert_eq!(PAYLOAD.len(), 8_369);
    assert_eq!(
        sha256_hex(PAYLOAD),
        "2d354f07af5fc18b408b72b7ccb91a565746977ff52de5a25872adaa25f207c9"
    );
    assert_eq!(RECEIPT.len(), 2_433);
    assert_eq!(
        sha256_hex(RECEIPT),
        "b79b6f508b5324762faaf7b3597d782056b8ecf1f8ce8ba498c0c14c83526719"
    );

    let summary = validate_redb_prescreen_pre_run_content(POLICY, PAYLOAD, RECEIPT).unwrap();
    assert_eq!(summary.payload_id, "redb-prescreen-synthetic-pre-run-v2");
    assert_eq!(
        summary.authorization,
        RedbPrescreenAuthorization::Unverified
    );
    assert!(!summary.execution_authorized());
    assert!(!summary.result_sealing_authorized());
}

#[test]
fn payload_whitespace_or_line_ending_drift_breaks_the_fixed_receipt_binding() {
    let mut appended = PAYLOAD.to_vec();
    appended.push(b' ');
    assert!(validate_redb_prescreen_pre_run_content(POLICY, &appended, RECEIPT).is_err());

    let crlf = String::from_utf8(PAYLOAD.to_vec())
        .unwrap()
        .replace('\n', "\r\n")
        .into_bytes();
    assert!(validate_redb_prescreen_pre_run_content(POLICY, &crlf, RECEIPT).is_err());
}
