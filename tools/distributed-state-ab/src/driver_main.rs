#![forbid(unsafe_code)]

use std::fs::OpenOptions;
use std::io::{Read as _, Write as _};
use std::path::{Path, PathBuf};
use std::process::ExitCode;
use std::str::FromStr as _;

use distributed_state_ab::{
    materialize_driver_trace, seal_base_plan, validate_manifest_path, verify_current_executable,
    Arm, DryRunRecordV1, ObserverBehavior, ObserverDispositionV1, DRY_RUN_RECORD_SCHEMA, NOTICE,
};

mod driver_supervisor;
mod persisted_end_seal;

use driver_supervisor::ObserverSupervisor;
use persisted_end_seal::validate_persist_and_seal;

const BASE_PLAN_FILE: &str = "base-plan.json";
const DRIVER_TRACE_FILE: &str = "driver-trace.json";
const RESULT_FILE: &str = "dry-run-record.json";

fn main() -> ExitCode {
    println!("{NOTICE}");
    let _ = std::io::stdout().flush();
    match run() {
        Ok(summary) => {
            println!(
                "SCHEDULE_SCAFFOLD_DRY_RUN_OK arm={} observer={} plan_sha256={} trace_sha256={}",
                summary.arm.label(),
                summary.disposition_label,
                summary.plan_sha256,
                summary.trace_sha256
            );
            ExitCode::SUCCESS
        }
        Err(error) => {
            eprintln!("INVALID_DRY_RUN {error}");
            ExitCode::from(2)
        }
    }
}

struct Summary {
    arm: Arm,
    disposition_label: &'static str,
    plan_sha256: String,
    trace_sha256: String,
}

fn run() -> Result<Summary, String> {
    let mut arguments = std::env::args_os();
    let _program = arguments.next();
    if arguments.next().as_deref() != Some(std::ffi::OsStr::new("dry-run")) {
        return Err(usage());
    }
    let manifest_argument = arguments.next().ok_or_else(usage)?;
    let artifact_argument = arguments.next().ok_or_else(usage)?;
    let arm_argument = arguments.next().ok_or_else(usage)?;
    let behavior_argument = arguments.next().ok_or_else(usage)?;
    if arguments.next().is_some() {
        return Err(usage());
    }

    let arm = Arm::from_str(
        arm_argument
            .to_str()
            .ok_or_else(|| "arm is not valid Unicode".to_owned())?,
    )
    .map_err(|error| error.to_string())?;
    let behavior = ObserverBehavior::from_str(
        behavior_argument
            .to_str()
            .ok_or_else(|| "observer behavior is not valid Unicode".to_owned())?,
    )
    .map_err(|error| error.to_string())?;
    let manifest_path = PathBuf::from(manifest_argument)
        .canonicalize()
        .map_err(|error| format!("canonicalize manifest: {error}"))?;
    let artifact_directory = PathBuf::from(artifact_argument);
    if !artifact_directory.is_absolute() {
        return Err("artifact directory must be absolute".to_owned());
    }

    let manifest = validate_manifest_path(&manifest_path).map_err(|error| error.to_string())?;
    verify_current_executable(&manifest.artifacts().driver).map_err(|error| error.to_string())?;
    let plan = seal_base_plan(&manifest).map_err(|error| error.to_string())?;

    std::fs::create_dir(&artifact_directory).map_err(|error| {
        format!(
            "create exclusive artifact directory {}: {error}",
            artifact_directory.display()
        )
    })?;
    write_new(
        &artifact_directory.join(BASE_PLAN_FILE),
        plan.canonical_bytes(),
    )?;

    let supervisor = ObserverSupervisor::spawn(
        &manifest,
        &manifest_path,
        &artifact_directory,
        arm,
        behavior,
    );

    // This only serializes the frozen schedule; it does not execute a workload or contact a SUT.
    // It receives no observer status, capture receiver, or result channel.
    let completed = materialize_driver_trace(&plan).map_err(|error| error.to_string())?;
    let end_seal = validate_persist_and_seal(
        &artifact_directory.join(DRIVER_TRACE_FILE),
        &completed,
        &plan,
    )?;

    let collection = supervisor.collect(end_seal, &manifest, &plan, arm)?;
    verify_exact_file(
        &artifact_directory.join(BASE_PLAN_FILE),
        plan.canonical_bytes(),
    )?;
    verify_exact_file(
        &artifact_directory.join(DRIVER_TRACE_FILE),
        completed.canonical_bytes(),
    )?;
    let record = DryRunRecordV1 {
        schema_version: DRY_RUN_RECORD_SCHEMA.to_owned(),
        notice: NOTICE.to_owned(),
        execution_eligible: false,
        attempt_id: manifest.manifest().attempt_id.clone(),
        arm,
        injected_observer_behavior: behavior.label().to_owned(),
        raw_manifest_sha256: manifest.raw_sha256().to_owned(),
        canonical_manifest_sha256: manifest.canonical_sha256().to_owned(),
        base_plan_sha256: plan.sha256().to_owned(),
        driver_schedule_sha256: plan.driver_schedule_sha256().to_owned(),
        observer_schedule_sha256: plan.observer_schedule_sha256().to_owned(),
        driver_trace_sha256: collection.end.trace_sha256.clone(),
        driver_sha256: manifest.artifacts().driver.sha256.clone(),
        observer_sha256: manifest.artifacts().observer.sha256.clone(),
        artifact_digests: plan.plan().artifact_digests.clone(),
        observer_pid: collection.pid,
        action_count: collection.end.action_count,
        scheduled_end_ns: collection.end.scheduled_end_ns,
        trace_matches_plan: true,
        observer_outcome_consumed_only_after_end: collection.outcome_consumed_only_after_end,
        observer_disposition: collection.disposition,
        observer_stdout_retained_bytes: collection.stdout_bytes,
        observer_stdout_retained_sha256: collection.stdout_sha256,
        observer_stderr_retained_bytes: collection.stderr_bytes,
        observer_stderr_retained_sha256: collection.stderr_sha256,
        supervisor_events: collection.events,
    };
    let record_bytes =
        serde_json::to_vec(&record).map_err(|error| format!("encode dry-run record: {error}"))?;
    write_new(&artifact_directory.join(RESULT_FILE), &record_bytes)?;

    Ok(Summary {
        arm,
        disposition_label: disposition_label(record.observer_disposition),
        plan_sha256: plan.sha256().to_owned(),
        trace_sha256: record.driver_trace_sha256.clone(),
    })
}

fn usage() -> String {
    "usage: distributed-state-ab-driver dry-run <manifest.json> <new-artifact-dir> \
     <C|D> <success|exit|hang|malformed>"
        .to_owned()
}

fn write_new(path: &Path, bytes: &[u8]) -> Result<(), String> {
    let mut file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(path)
        .map_err(|error| format!("create artifact {}: {error}", path.display()))?;
    file.write_all(bytes)
        .map_err(|error| format!("write artifact {}: {error}", path.display()))?;
    file.sync_all()
        .map_err(|error| format!("sync artifact {}: {error}", path.display()))
}

fn verify_exact_file(path: &Path, expected: &[u8]) -> Result<(), String> {
    let file = std::fs::File::open(path)
        .map_err(|error| format!("reopen artifact {}: {error}", path.display()))?;
    let metadata = file
        .metadata()
        .map_err(|error| format!("inspect artifact {}: {error}", path.display()))?;
    if !metadata.is_file() || metadata.len() != expected.len() as u64 {
        return Err(format!(
            "artifact changed after persistence: {}",
            path.display()
        ));
    }
    let mut observed = Vec::with_capacity(expected.len());
    file.take(expected.len() as u64 + 1)
        .read_to_end(&mut observed)
        .map_err(|error| format!("reread artifact {}: {error}", path.display()))?;
    if observed != expected {
        return Err(format!(
            "artifact changed after persistence: {}",
            path.display()
        ));
    }
    Ok(())
}

fn disposition_label(disposition: ObserverDispositionV1) -> &'static str {
    match disposition {
        ObserverDispositionV1::Valid => "valid",
        ObserverDispositionV1::ExitNonzero => "exit_nonzero",
        ObserverDispositionV1::HungKilled => "hung_killed",
        ObserverDispositionV1::Malformed => "malformed",
        ObserverDispositionV1::OutputOversized => "output_oversized",
        ObserverDispositionV1::SpawnFailed => "spawn_failed",
        ObserverDispositionV1::StartSignalFailed => "start_signal_failed",
        ObserverDispositionV1::TerminationFailed => "termination_failed",
        ObserverDispositionV1::CaptureIncomplete => "capture_incomplete",
    }
}
