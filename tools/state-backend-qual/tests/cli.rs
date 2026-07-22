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
    assert_eq!(
        String::from_utf8_lossy(&output.stdout),
        format!(
            "{NOTICE}\nVALID_INELIGIBLE_PROFILE schema=distributed-state-qual/v1 \
             profile=linux-nvme-v1 status=candidate_unapproved\n"
        )
    );
    assert!(output.stderr.is_empty());
}

#[test]
fn validates_only_a_matching_model_result() {
    let profile = manifest_path("profiles/linux-nvme-v1.candidate.json");
    let result = manifest_path("tests/fixtures/model-result-aggregate-v1.json");
    let output = binary()
        .arg("validate-model-result")
        .arg(&profile)
        .arg(&result)
        .output()
        .unwrap();
    assert!(output.status.success());
    assert_eq!(
        String::from_utf8_lossy(&output.stdout),
        format!("{NOTICE}\nVALID_INELIGIBLE_MODEL_RESULT profile=linux-nvme-v1 requests=2\n")
    );
    assert!(output.stderr.is_empty());

    let output = binary()
        .arg("validate-model-result")
        .arg(profile)
        .arg(manifest_path("schema/model-result-v1.schema.json"))
        .output()
        .unwrap();
    assert_eq!(output.status.code(), Some(2));
    assert_eq!(
        String::from_utf8_lossy(&output.stdout),
        format!("{NOTICE}\n")
    );
    assert!(String::from_utf8_lossy(&output.stderr).starts_with("INVALID_MODEL_RESULT"));
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

#[test]
fn oversized_file_is_read_only_to_the_validation_cap() {
    let path = std::env::temp_dir().join(format!(
        "state-backend-qual-oversized-{}.json",
        std::process::id()
    ));
    std::fs::write(&path, vec![b' '; state_backend_qual::MAX_PROFILE_BYTES + 2]).unwrap();
    let output = binary()
        .arg("validate-profile")
        .arg(&path)
        .output()
        .unwrap();
    std::fs::remove_file(path).unwrap();

    assert_eq!(output.status.code(), Some(2));
    assert_eq!(
        String::from_utf8_lossy(&output.stdout),
        format!("{NOTICE}\n")
    );
    assert!(String::from_utf8_lossy(&output.stderr).contains("maximum is 1048576"));
}
