use std::path::PathBuf;
use std::process::Command;

const NOTICE: &str = "NOT CERTIFICATION EVIDENCE";

fn binary() -> Command {
    Command::new(env!("CARGO_BIN_EXE_independent-soak-contract"))
}

fn manifest_path(relative: &str) -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join(relative)
}

#[test]
fn valid_draft_prints_ineligible_notice() {
    let output = binary()
        .arg("validate-contract")
        .arg(manifest_path("contracts/draft-v1alpha1.json"))
        .output()
        .unwrap();
    assert!(output.status.success());
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.starts_with(NOTICE));
    assert!(stdout.contains("VALID_INELIGIBLE_DRAFT"));
}

#[test]
fn fixture_check_prints_non_certification_notice() {
    let output = binary()
        .arg("verify-oracle-fixture")
        .arg(manifest_path("fixtures/grouped-count-sum-alo-v1.json"))
        .output()
        .unwrap();
    assert!(output.status.success());
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.starts_with(NOTICE));
    assert!(stdout.contains("ORACLE_FIXTURE_OK"));
}

#[test]
fn v2_fixture_dispatch_prints_non_certification_notice() {
    let output = binary()
        .arg("verify-oracle-fixture")
        .arg(manifest_path("fixtures/grouped-count-sum-alo-v2.json"))
        .output()
        .unwrap();
    assert!(output.status.success());
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.starts_with(NOTICE));
    assert!(stdout
        .contains("ORACLE_FIXTURE_OK cases=1 model_matches=1 product_failures=0 invalid_runs=0"));
}

#[test]
fn unsupported_fixture_version_is_rejected_explicitly() {
    let output = binary()
        .arg("verify-oracle-fixture")
        .arg(manifest_path("contracts/draft-v1alpha1.json"))
        .output()
        .unwrap();
    assert!(!output.status.success());
    assert!(String::from_utf8_lossy(&output.stdout).starts_with(NOTICE));
    assert!(String::from_utf8_lossy(&output.stderr)
        .contains("unsupported oracle fixture schema_version"));
}

#[test]
fn errors_still_print_non_certification_notice() {
    let output = binary()
        .arg("validate-contract")
        .arg(manifest_path("contracts/does-not-exist.json"))
        .output()
        .unwrap();
    assert!(!output.status.success());
    assert!(String::from_utf8_lossy(&output.stdout).starts_with(NOTICE));
}

#[test]
fn semantic_validation_failure_flushes_notice_before_error() {
    let output = binary()
        .arg("validate-contract")
        .arg(manifest_path("schema/contract-v1alpha1.schema.json"))
        .output()
        .unwrap();
    assert!(!output.status.success());
    assert_eq!(
        String::from_utf8_lossy(&output.stdout),
        format!("{NOTICE}\n")
    );
    assert!(String::from_utf8_lossy(&output.stderr).starts_with("INVALID_DRAFT"));
}
