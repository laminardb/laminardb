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
fn candidate_neutral_v2_prints_ineligible_notice() {
    let output = binary()
        .arg("validate-profile")
        .arg(manifest_path("profiles/linux-nvme-v2.candidate.json"))
        .output()
        .unwrap();
    assert!(output.status.success());
    assert_eq!(
        String::from_utf8_lossy(&output.stdout),
        format!(
            "{NOTICE}\nVALID_INELIGIBLE_PROFILE schema=distributed-state-qual/v2 \
             profile=linux-nvme-v2 status=candidate_unapproved\n"
        )
    );
    assert!(output.stderr.is_empty());
}

#[test]
fn candidate_neutral_v3_prints_ineligible_notice() {
    let output = binary()
        .arg("validate-profile")
        .arg(manifest_path("profiles/linux-nvme-v3.candidate.json"))
        .output()
        .unwrap();
    assert!(output.status.success());
    assert_eq!(
        String::from_utf8_lossy(&output.stdout),
        format!(
            "{NOTICE}\nVALID_INELIGIBLE_PROFILE schema=distributed-state-qual/v3 \
             profile=linux-nvme-v3 status=candidate_unapproved\n"
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
fn validates_only_an_exact_profile_bound_mechanism_mapping() {
    let profile = manifest_path("profiles/linux-nvme-v3.candidate.json");
    let mapping = manifest_path("tests/fixtures/mechanism-mapping-observed-v1.json");
    let output = binary()
        .arg("validate-mechanism-mapping")
        .arg(&profile)
        .arg(&mapping)
        .output()
        .unwrap();
    assert!(output.status.success());
    assert_eq!(
        String::from_utf8_lossy(&output.stdout),
        format!(
            "{NOTICE}\nVALID_INELIGIBLE_MECHANISM_MAPPING mapping=synthetic-observed-v1 \
             candidate=synthetic-candidate debt=observed stalls=observed\n"
        )
    );
    assert!(output.stderr.is_empty());

    let output = binary()
        .arg("validate-mechanism-mapping")
        .arg(manifest_path("profiles/linux-nvme-v2.candidate.json"))
        .arg(mapping)
        .output()
        .unwrap();
    assert_eq!(output.status.code(), Some(2));
    assert!(String::from_utf8_lossy(&output.stderr).starts_with("INVALID_MECHANISM_MAPPING"));
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

#[test]
fn redb_review_content_is_valid_but_explicitly_authorization_unverified() {
    let fixture = |name: &str| {
        manifest_path(&format!(
            "tests/fixtures/redb-prescreen-successor-v1/{name}.json"
        ))
    };
    let output = binary()
        .arg("validate-redb-prescreen-pre-run-content")
        .arg(fixture("policy"))
        .arg(fixture("payload"))
        .arg(fixture("receipt"))
        .output()
        .unwrap();

    assert!(output.status.success());
    assert_eq!(
        String::from_utf8_lossy(&output.stdout),
        format!(
            "{NOTICE}\nVALID_INELIGIBLE_REDB_PRESCREEN_CONTENT stage=pre_run \
             payload=redb-prescreen-synthetic-pre-run-v1 \
             authorization=authorization_unverified\n"
        )
    );
    assert!(output.stderr.is_empty());

    let complete_output = format!(
        "{}{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    for forbidden in [
        "PRESCREEN_PASS",
        "PRESCREEN_NO_GO",
        "REJECT_EXACT_PIN",
        "SMOKE_PASS",
        "authorization=verified",
        "execution_authorized=true",
        "result_sealed=true",
    ] {
        assert!(!complete_output.contains(forbidden));
    }
}

#[test]
fn redb_post_run_binding_is_opaque_ineligible_and_authorization_unverified() {
    let fixture = |name: &str| {
        manifest_path(&format!(
            "tests/fixtures/redb-prescreen-successor-v1/{name}.json"
        ))
    };
    let output = binary()
        .arg("validate-redb-prescreen-post-run-binding")
        .arg(fixture("policy"))
        .arg(fixture("payload"))
        .arg(fixture("receipt"))
        .arg(fixture("opaque-result-payload"))
        .arg(fixture("opaque-artifact-index"))
        .arg(fixture("post-run-receipt"))
        .output()
        .unwrap();

    assert!(output.status.success());
    assert_eq!(
        String::from_utf8_lossy(&output.stdout),
        format!(
            "{NOTICE}\nVALID_INELIGIBLE_REDB_PRESCREEN_BINDING stage=post_run \
             authorization=authorization_unverified\n"
        )
    );
    assert!(output.stderr.is_empty());

    let complete_output = format!(
        "{}{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    for forbidden in [
        "DEFER",
        "SMOKE_PASS",
        "PRESCREEN_PASS",
        "PRESCREEN_NO_GO",
        "REJECT_EXACT_PIN",
        "provider_state",
        "APPROVED",
        "result_sealed",
        "backend_selection",
        "qualification_eligible",
    ] {
        assert!(!complete_output.contains(forbidden));
    }
}

#[test]
fn redb_post_run_binding_rejects_wrong_bytes_and_oversized_inputs() {
    let fixture = |name: &str| {
        manifest_path(&format!(
            "tests/fixtures/redb-prescreen-successor-v1/{name}.json"
        ))
    };
    let output = binary()
        .arg("validate-redb-prescreen-post-run-binding")
        .arg(fixture("policy"))
        .arg(fixture("payload"))
        .arg(fixture("receipt"))
        .arg(fixture("opaque-artifact-index"))
        .arg(fixture("opaque-result-payload"))
        .arg(fixture("post-run-receipt"))
        .output()
        .unwrap();
    assert_eq!(output.status.code(), Some(2));
    assert_eq!(
        String::from_utf8_lossy(&output.stdout),
        format!("{NOTICE}\n")
    );
    assert!(String::from_utf8_lossy(&output.stderr).starts_with("INVALID_REDB_PRESCREEN_BINDING "));

    let oversized = std::env::temp_dir().join(format!(
        "state-backend-qual-redb-result-{}.bin",
        std::process::id()
    ));
    std::fs::write(
        &oversized,
        vec![b'x'; state_backend_qual::redb_prescreen::MAX_REDB_RESULT_PAYLOAD_BYTES + 1],
    )
    .unwrap();
    let output = binary()
        .arg("validate-redb-prescreen-post-run-binding")
        .arg(fixture("policy"))
        .arg(fixture("payload"))
        .arg(fixture("receipt"))
        .arg(&oversized)
        .arg(fixture("opaque-artifact-index"))
        .arg(fixture("post-run-receipt"))
        .output()
        .unwrap();
    std::fs::remove_file(oversized).unwrap();
    assert_eq!(output.status.code(), Some(2));
    assert_eq!(
        String::from_utf8_lossy(&output.stdout),
        format!("{NOTICE}\n")
    );
    assert!(String::from_utf8_lossy(&output.stderr).starts_with("INVALID_INPUT "));
    assert!(String::from_utf8_lossy(&output.stderr).contains("exceeds maximum of 262144 bytes"));
}

#[test]
fn redb_review_cli_rejects_wrong_shapes_and_has_no_run_command() {
    let fixture = |name: &str| {
        manifest_path(&format!(
            "tests/fixtures/redb-prescreen-successor-v1/{name}.json"
        ))
    };
    let output = binary()
        .arg("validate-redb-prescreen-pre-run-content")
        .arg(fixture("policy"))
        .arg(fixture("payload"))
        .arg(fixture("payload"))
        .output()
        .unwrap();
    assert_eq!(output.status.code(), Some(2));
    assert_eq!(
        String::from_utf8_lossy(&output.stdout),
        format!("{NOTICE}\n")
    );
    assert!(String::from_utf8_lossy(&output.stderr).starts_with("INVALID_REDB_PRESCREEN_CONTENT "));

    for command in [
        "run-redb-prescreen",
        "dispatch-redb-prescreen",
        "authorize-redb-prescreen",
        "approve-redb-prescreen",
        "accept-redb-prescreen-result",
        "verify-redb-prescreen-result",
        "classify-redb-prescreen-result",
        "seal-redb-prescreen-result",
        "select-redb-backend",
        "qualify-redb-backend",
    ] {
        let output = binary().arg(command).output().unwrap();
        assert_eq!(output.status.code(), Some(64));
        assert_eq!(
            String::from_utf8_lossy(&output.stdout),
            format!("{NOTICE}\n")
        );
        assert!(String::from_utf8_lossy(&output.stderr).starts_with("usage:"));
    }
}
