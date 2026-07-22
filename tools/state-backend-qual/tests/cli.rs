use std::path::PathBuf;
use std::process::Command;

const NOTICE: &str = "NOT QUALIFICATION EVIDENCE";

fn binary() -> Command {
    Command::new(env!("CARGO_BIN_EXE_state-backend-qual"))
}

fn manifest_path(relative: &str) -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join(relative)
}

#[test]
fn candidate_prints_ineligible_notice() {
    let output = binary()
        .arg("validate-profile")
        .arg(manifest_path("profiles/linux-nvme-v1.candidate.json"))
        .output()
        .unwrap();
    assert!(output.status.success());
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.starts_with(NOTICE));
    assert!(stdout.contains("VALID_INELIGIBLE_PROFILE"));
    assert!(!stdout.contains("QUALIFIED"));
}

#[test]
fn invalid_input_still_prints_notice_and_returns_two() {
    let output = binary()
        .arg("validate-profile")
        .arg(manifest_path("profiles/does-not-exist.json"))
        .output()
        .unwrap();
    assert_eq!(output.status.code(), Some(2));
    assert_eq!(
        String::from_utf8_lossy(&output.stdout),
        format!("{NOTICE}\n")
    );
    assert!(String::from_utf8_lossy(&output.stderr).starts_with("INVALID_INPUT"));
}

#[test]
fn usage_error_returns_sixty_four_after_notice() {
    let output = binary().output().unwrap();
    assert_eq!(output.status.code(), Some(64));
    assert_eq!(
        String::from_utf8_lossy(&output.stdout),
        format!("{NOTICE}\n")
    );
    assert!(String::from_utf8_lossy(&output.stderr).starts_with("usage:"));
}
