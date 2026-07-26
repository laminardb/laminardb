use std::path::{Path, PathBuf};
use std::process::{Command, Output};

use distributed_state_ab::{
    sha256_bytes, ArtifactIdentityV1, ArtifactSetV1, AuthenticationV1, BasePlanV1,
    DiagnosticRouteV1, DryRunRecordV1, LifecycleBoundaryV1, LimitsV1, ManifestV1,
    ObserverDispositionV1, ObserverMode, ObserverPolicyV1, ObserverResultV1, PairV1, WorkloadV1,
    MANIFEST_SCHEMA, NOTICE,
};
use serde_json::Value;
use tempfile::TempDir;

const SECRET_SENTINEL: &str = "cycle62-secret-7f52d1d9b9874a73a869";

struct Fixture {
    _directory: TempDir,
    root: PathBuf,
    driver: PathBuf,
    manifest: ManifestV1,
    manifest_path: PathBuf,
}

struct Run {
    output: Output,
    record: DryRunRecordV1,
    plan: Vec<u8>,
    trace: Vec<u8>,
    observer_stdout: Vec<u8>,
    observer_stderr: Vec<u8>,
    artifact_bytes: Vec<u8>,
}

impl Fixture {
    fn new() -> Self {
        let directory = tempfile::tempdir().unwrap();
        let root = directory.path().join("fixture with spaces");
        std::fs::create_dir(&root).unwrap();
        let driver = PathBuf::from(env!("CARGO_BIN_EXE_distributed-state-ab-driver"))
            .canonicalize()
            .unwrap();
        let observer = PathBuf::from(env!("CARGO_BIN_EXE_distributed-state-ab-observer"))
            .canonicalize()
            .unwrap();
        let server = write_fixture(&root, "server.bin", b"shared-cd-server");
        let trace = write_fixture(&root, "trace.json", b"{\"records\":80000}");
        let config = write_fixture(&root, "config.json", b"{\"redacted\":true}");
        let dependencies = write_fixture(&root, "dependencies.json", b"{\"images\":[]}");
        let virtual_script = write_fixture(
            &root,
            "virtual-control.json",
            b"{\"outcomes\":\"fixed-success\"}",
        );
        let protocol = write_fixture(&root, "protocol.md", b"cycle-60-protocol-v1");
        let manifest = ManifestV1 {
            schema_version: MANIFEST_SCHEMA.to_owned(),
            notice: NOTICE.to_owned(),
            execution_eligible: false,
            purpose: "engineering_instrumentation_ab_nonfeedback_dry_run".to_owned(),
            attempt_id: "cycle62-dry-run".to_owned(),
            artifacts: ArtifactSetV1 {
                driver: artifact(&driver),
                observer: artifact(&observer),
                server: artifact(&server),
                trace_manifest: artifact(&trace),
                redacted_config: artifact(&config),
                dependency_manifest: artifact(&dependencies),
                virtual_control_script: artifact(&virtual_script),
                protocol_spec: artifact(&protocol),
            },
            node_ordinals: [0, 1, 2],
            workload: WorkloadV1 {
                record_count: 80_000,
                records_per_second: 400,
                input_target_end_ns: 200_000_000_000,
            },
            observer_policy: ObserverPolicyV1 {
                poll_interval_ns: 5_000_000_000,
                local_evidence_max_bytes: 4 * 1_024,
                exact_timing_max_bytes: 64 * 1_024,
            },
            limits: LimitsV1 {
                max_driver_actions: 104,
                max_observer_slots: 58,
                observer_stdout_max_bytes: 512 * 1_024,
                observer_stderr_max_bytes: 16 * 1_024,
                observer_completion_timeout_ms: 2_000,
            },
            pair: PairV1 {
                control: ObserverMode::Suppress,
                treatment: ObserverMode::Poll,
                shared_server: true,
                shared_config: true,
            },
            authentication: AuthenticationV1::SyntheticNone,
        };
        let manifest_path = root.join("manifest.json");
        std::fs::write(&manifest_path, serde_json::to_vec(&manifest).unwrap()).unwrap();
        Self {
            _directory: directory,
            root,
            driver,
            manifest,
            manifest_path,
        }
    }

    fn run(&self, arm: &str, behavior: &str, name: &str) -> Run {
        let artifact_directory = self.root.join(name);
        let output = Command::new(&self.driver)
            .arg("dry-run")
            .arg(&self.manifest_path)
            .arg(&artifact_directory)
            .arg(arm)
            .arg(behavior)
            .env("LAMINAR_AB_SECRET_SENTINEL", SECRET_SENTINEL)
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "stdout={} stderr={}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
        let record: DryRunRecordV1 = serde_json::from_slice(
            &std::fs::read(artifact_directory.join("dry-run-record.json")).unwrap(),
        )
        .unwrap();
        let plan = std::fs::read(artifact_directory.join("base-plan.json")).unwrap();
        let trace = std::fs::read(artifact_directory.join("driver-trace.json")).unwrap();
        let observer_stdout = std::fs::read(artifact_directory.join("observer.stdout")).unwrap();
        let observer_stderr = std::fs::read(artifact_directory.join("observer.stderr")).unwrap();
        let mut artifact_bytes = Vec::new();
        for entry in std::fs::read_dir(&artifact_directory).unwrap() {
            let path = entry.unwrap().path();
            if path.is_file() {
                artifact_bytes.extend(std::fs::read(path).unwrap());
            }
        }
        Run {
            output,
            record,
            plan,
            trace,
            observer_stdout,
            observer_stderr,
            artifact_bytes,
        }
    }
}

fn write_fixture(root: &Path, name: &str, bytes: &[u8]) -> PathBuf {
    let path = root.join(name);
    std::fs::write(&path, bytes).unwrap();
    path.canonicalize().unwrap()
}

fn artifact(path: &Path) -> ArtifactIdentityV1 {
    let bytes = std::fs::read(path).unwrap();
    ArtifactIdentityV1 {
        path: path.canonicalize().unwrap(),
        byte_length: bytes.len() as u64,
        sha256: sha256_bytes(&bytes),
    }
}

fn fixture_hash(path: &Path) -> String {
    sha256_bytes(&std::fs::read(path).unwrap())
}

#[test]
fn observer_outcomes_and_arms_cannot_change_the_driver_trace() {
    let fixture = Fixture::new();
    let mut expected_plan = None;
    let mut expected_trace = None;
    let mut expected_plan_hash = None;
    let mut expected_trace_hash = None;
    let mut expected_artifact_digests = None;
    let mut control_result = None;
    let mut treatment_result = None;

    for arm in ["C", "D"] {
        for behavior in ["success", "exit", "hang", "malformed"] {
            let run = fixture.run(arm, behavior, &format!("run-{arm}-{behavior}"));
            assert!(String::from_utf8_lossy(&run.output.stdout).starts_with(NOTICE));
            assert!(!String::from_utf8_lossy(&run.output.stdout).contains(SECRET_SENTINEL));
            assert!(!String::from_utf8_lossy(&run.output.stderr).contains(SECRET_SENTINEL));
            assert!(!run
                .artifact_bytes
                .windows(SECRET_SENTINEL.len())
                .any(|window| window == SECRET_SENTINEL.as_bytes()));
            assert!(run.record.trace_matches_plan);
            assert!(run.record.observer_outcome_consumed_only_after_end);
            assert_eq!(run.record.action_count, 104);
            assert_eq!(run.record.scheduled_end_ns, 290_000_000_000);
            assert!(!run.record.execution_eligible);
            assert_eq!(run.record.notice, NOTICE);
            assert_eq!(run.record.injected_observer_behavior, behavior);
            assert!(run.record.observer_pid.is_some());
            let expected_disposition = match behavior {
                "success" => ObserverDispositionV1::Valid,
                "exit" => ObserverDispositionV1::ExitNonzero,
                "hang" => ObserverDispositionV1::HungKilled,
                "malformed" => ObserverDispositionV1::Malformed,
                _ => unreachable!(),
            };
            assert_eq!(run.record.observer_disposition, expected_disposition);

            let end_index = run
                .record
                .supervisor_events
                .iter()
                .position(|event| event == "end_seal_consumed")
                .unwrap();
            for (index, event) in run.record.supervisor_events.iter().enumerate() {
                if event.contains("status")
                    || event.contains("killed")
                    || event.contains("capture_consumed")
                {
                    assert!(index > end_index, "{event:?} occurred before end seal");
                }
            }

            match &expected_plan {
                Some(expected) => assert_eq!(&run.plan, expected),
                None => expected_plan = Some(run.plan.clone()),
            }
            match &expected_trace {
                Some(expected) => assert_eq!(&run.trace, expected),
                None => expected_trace = Some(run.trace.clone()),
            }
            match &expected_plan_hash {
                Some(expected) => assert_eq!(&run.record.base_plan_sha256, expected),
                None => expected_plan_hash = Some(run.record.base_plan_sha256.clone()),
            }
            match &expected_trace_hash {
                Some(expected) => assert_eq!(&run.record.driver_trace_sha256, expected),
                None => expected_trace_hash = Some(run.record.driver_trace_sha256.clone()),
            }
            match &expected_artifact_digests {
                Some(expected) => assert_eq!(&run.record.artifact_digests, expected),
                None => expected_artifact_digests = Some(run.record.artifact_digests.clone()),
            }

            if behavior == "success" {
                let result: ObserverResultV1 =
                    serde_json::from_slice(&run.observer_stdout).unwrap();
                if arm == "C" {
                    control_result = Some(result);
                } else {
                    treatment_result = Some(result);
                }
            }
            assert_eq!(
                run.record.observer_stdout_retained_sha256,
                sha256_bytes(&run.observer_stdout)
            );
            assert_eq!(
                run.record.observer_stdout_retained_bytes as usize,
                run.observer_stdout.len()
            );
            assert_eq!(
                run.record.observer_stderr_retained_sha256,
                sha256_bytes(&run.observer_stderr)
            );
            assert_eq!(
                run.record.observer_stderr_retained_bytes as usize,
                run.observer_stderr.len()
            );
        }
    }

    let control = control_result.unwrap();
    let treatment = treatment_result.unwrap();
    assert_eq!(control.mode, ObserverMode::Suppress);
    assert_eq!(treatment.mode, ObserverMode::Poll);
    assert_eq!(
        control.observer_schedule_sha256,
        treatment.observer_schedule_sha256
    );
    assert_eq!(control.scheduled_slots, 58);
    assert_eq!(treatment.scheduled_slots, 58);
    assert_eq!(control.suppressed_probes, 348);
    assert!(control.planned_probes.is_empty());
    assert_eq!(treatment.suppressed_probes, 0);
    assert_eq!(treatment.planned_probes.len(), 348);
    let first = &treatment.planned_probes[0];
    assert_eq!(first.slot_ordinal, 0);
    assert_eq!(first.at_ns, 0);
    assert_eq!(first.boundary, Some(LifecycleBoundaryV1::WindowStart));
    assert_eq!(first.node_ordinal, 0);
    assert_eq!(first.route, DiagnosticRouteV1::LocalEvidence);
    let second = &treatment.planned_probes[1];
    assert_eq!(second.route, DiagnosticRouteV1::ExactTiming);
    let last = treatment.planned_probes.last().unwrap();
    assert_eq!(last.slot_ordinal, 57);
    assert_eq!(last.at_ns, 285_000_000_000);
    assert_eq!(last.boundary, None);
    assert_eq!(last.node_ordinal, 2);
    assert_eq!(last.route, DiagnosticRouteV1::ExactTiming);

    let plan: BasePlanV1 = serde_json::from_slice(expected_plan.as_ref().unwrap()).unwrap();
    assert_eq!(plan.driver_schedule.actions.len(), 104);
    assert_eq!(plan.observer_schedule.slots.len(), 58);
    assert_eq!(
        plan.source_manifest_sha256,
        fixture_hash(&fixture.manifest_path)
    );
    assert_eq!(
        Some(plan.artifact_digests.clone()),
        expected_artifact_digests
    );
    let boundaries: Vec<_> = plan
        .observer_schedule
        .slots
        .iter()
        .filter_map(|slot| slot.boundary.map(|boundary| (slot.at_ns, boundary)))
        .collect();
    assert_eq!(
        boundaries,
        vec![
            (0, LifecycleBoundaryV1::WindowStart),
            (120_000_000_000, LifecycleBoundaryV1::FaultCheckpoint),
            (200_000_000_000, LifecycleBoundaryV1::InputTargetEnd),
            (
                255_000_000_000,
                LifecycleBoundaryV1::PostRecoverySamplingAnchor
            ),
        ]
    );
}

#[test]
fn artifact_directory_is_exclusive_and_cannot_be_truncated() {
    let fixture = Fixture::new();
    let first = fixture.run("C", "success", "exclusive-run");
    let directory = fixture.root.join("exclusive-run");
    let before = sha256_bytes(&std::fs::read(directory.join("driver-trace.json")).unwrap());
    let second = Command::new(&fixture.driver)
        .arg("dry-run")
        .arg(&fixture.manifest_path)
        .arg(&directory)
        .arg("D")
        .arg("success")
        .output()
        .unwrap();
    assert!(!second.status.success());
    assert!(String::from_utf8_lossy(&second.stderr).contains("exclusive artifact directory"));
    assert_eq!(
        sha256_bytes(&std::fs::read(directory.join("driver-trace.json")).unwrap()),
        before
    );
    assert_eq!(
        first.record.observer_disposition,
        ObserverDispositionV1::Valid
    );
}

#[test]
fn minimum_stdout_cap_accepts_the_official_treatment_output() {
    let mut fixture = Fixture::new();
    fixture.manifest.limits.observer_stdout_max_bytes = 256 * 1_024;
    std::fs::write(
        &fixture.manifest_path,
        serde_json::to_vec(&fixture.manifest).unwrap(),
    )
    .unwrap();
    let run = fixture.run("D", "success", "minimum-cap-run");
    assert_eq!(
        run.record.observer_disposition,
        ObserverDispositionV1::Valid
    );
    assert!(run.observer_stdout.len() <= 256 * 1_024);
}

#[test]
fn manifest_rejects_credentials_unknown_fields_and_excessive_policy_before_spawn() {
    let fixture = Fixture::new();
    let original: Value =
        serde_json::from_slice(&std::fs::read(&fixture.manifest_path).unwrap()).unwrap();

    let mut token = original.clone();
    token["token_file"] = Value::String("forbidden-secret".to_owned());
    let token_path = fixture.root.join("manifest-token.json");
    std::fs::write(&token_path, serde_json::to_vec(&token).unwrap()).unwrap();
    let token_output = Command::new(&fixture.driver)
        .arg("dry-run")
        .arg(&token_path)
        .arg(fixture.root.join("token-run"))
        .arg("C")
        .arg("success")
        .output()
        .unwrap();
    assert!(!token_output.status.success());
    assert!(!fixture.root.join("token-run").exists());

    let mut excessive = original;
    excessive["observer_policy"]["local_evidence_max_bytes"] = Value::from(4097);
    let excessive_path = fixture.root.join("manifest-excessive.json");
    std::fs::write(&excessive_path, serde_json::to_vec(&excessive).unwrap()).unwrap();
    let excessive_output = Command::new(&fixture.driver)
        .arg("dry-run")
        .arg(&excessive_path)
        .arg(fixture.root.join("excessive-run"))
        .arg("D")
        .arg("success")
        .output()
        .unwrap();
    assert!(!excessive_output.status.success());
    assert!(String::from_utf8_lossy(&excessive_output.stderr)
        .contains("local_evidence_max_bytes must be in 1..=4096"));
    assert!(!fixture.root.join("excessive-run").exists());
}

#[test]
fn changed_input_artifact_fails_before_artifact_directory_creation() {
    let fixture = Fixture::new();
    std::fs::write(
        &fixture.manifest.artifacts.redacted_config.path,
        b"{\"redacted\":false}",
    )
    .unwrap();
    let run_directory = fixture.root.join("substitution-run");
    let output = Command::new(&fixture.driver)
        .arg("dry-run")
        .arg(&fixture.manifest_path)
        .arg(&run_directory)
        .arg("C")
        .arg("success")
        .output()
        .unwrap();
    assert!(!output.status.success());
    assert!(String::from_utf8_lossy(&output.stderr).contains("identity mismatch"));
    assert!(!run_directory.exists());
}

#[test]
fn observer_spawn_failure_is_classified_only_after_the_same_trace_completes() {
    let fixture = Fixture::new();
    let baseline = fixture.run("D", "success", "spawn-baseline");
    let fake_observer = write_fixture(&fixture.root, "not-an-executable.txt", b"not executable");
    let mut manifest = fixture.manifest.clone();
    manifest.attempt_id = "cycle62-spawn-failure".to_owned();
    manifest.artifacts.observer = artifact(&fake_observer);
    let manifest_path = fixture.root.join("manifest-spawn-failure.json");
    std::fs::write(&manifest_path, serde_json::to_vec(&manifest).unwrap()).unwrap();
    let artifact_directory = fixture.root.join("spawn-failure-run");
    let output = Command::new(&fixture.driver)
        .arg("dry-run")
        .arg(&manifest_path)
        .arg(&artifact_directory)
        .arg("D")
        .arg("success")
        .output()
        .unwrap();
    assert!(output.status.success());
    let record: DryRunRecordV1 = serde_json::from_slice(
        &std::fs::read(artifact_directory.join("dry-run-record.json")).unwrap(),
    )
    .unwrap();
    assert_eq!(
        record.observer_disposition,
        ObserverDispositionV1::SpawnFailed
    );
    assert_eq!(record.observer_pid, None);
    assert!(record.observer_outcome_consumed_only_after_end);
    assert_eq!(
        std::fs::read(artifact_directory.join("driver-trace.json")).unwrap(),
        baseline.trace
    );
}
