#![forbid(unsafe_code)]

use std::io::{Read as _, Write as _};
use std::path::PathBuf;
use std::process::ExitCode;
use std::str::FromStr as _;
use std::time::Duration;

use distributed_state_ab::observer_protocol::{
    run_observer_protocol, ProtocolCancellation, SupervisorBootstrapSource,
};
use distributed_state_ab::{
    build_observer_result, seal_base_plan, validate_manifest_path, verify_current_executable, Arm,
    ObserverBehavior, NOTICE, START_SIGNAL_BYTES,
};

fn main() -> ExitCode {
    match run() {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("{NOTICE}: INVALID_OBSERVER_DRY_RUN {error}");
            ExitCode::from(2)
        }
    }
}

fn run() -> Result<(), String> {
    let mut arguments = std::env::args_os();
    let _program = arguments.next();
    let command = arguments.next().ok_or_else(usage)?;
    if std::env::vars_os().next().is_some() {
        return Err("observer environment must be empty".to_owned());
    }
    match command.to_str() {
        Some("dry-run") => run_dry_run(arguments),
        Some("fake-protocol") => run_fake_protocol(arguments),
        _ => Err(usage()),
    }
}

fn run_dry_run(mut arguments: impl Iterator<Item = std::ffi::OsString>) -> Result<(), String> {
    let manifest_argument = arguments.next().ok_or_else(usage)?;
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
            .ok_or_else(|| "behavior is not valid Unicode".to_owned())?,
    )
    .map_err(|error| error.to_string())?;

    match behavior {
        ObserverBehavior::Exit => std::process::exit(23),
        ObserverBehavior::Hang => loop {
            std::thread::sleep(Duration::from_secs(60));
        },
        ObserverBehavior::Success | ObserverBehavior::Malformed => {}
    }

    let manifest_path = PathBuf::from(manifest_argument)
        .canonicalize()
        .map_err(|error| format!("canonicalize manifest: {error}"))?;
    let manifest = validate_manifest_path(&manifest_path).map_err(|error| error.to_string())?;
    verify_current_executable(&manifest.artifacts().observer).map_err(|error| error.to_string())?;
    wait_for_start_signal()?;
    if behavior == ObserverBehavior::Malformed {
        print!("malformed-observer-output");
        std::io::stdout()
            .flush()
            .map_err(|error| format!("flush malformed output: {error}"))?;
        return Ok(());
    }
    let plan = seal_base_plan(&manifest).map_err(|error| error.to_string())?;
    let result = build_observer_result(&manifest, &plan, arm).map_err(|error| error.to_string())?;
    serde_json::to_writer(std::io::stdout().lock(), &result)
        .map_err(|error| format!("write observer result: {error}"))?;
    std::io::stdout()
        .flush()
        .map_err(|error| format!("flush observer result: {error}"))
}

fn run_fake_protocol(
    mut arguments: impl Iterator<Item = std::ffi::OsString>,
) -> Result<(), String> {
    let arm_argument = arguments.next().ok_or_else(usage)?;
    if arguments.next().is_some() {
        return Err(usage());
    }
    let arm = Arm::from_str(
        arm_argument
            .to_str()
            .ok_or_else(|| "arm is not valid Unicode".to_owned())?,
    )
    .map_err(|error| error.to_string())?;
    let stdin = std::io::stdin();
    let mut source = SupervisorBootstrapSource::new(stdin.lock());
    let input = source.take().map_err(|error| error.to_string())?;
    let result = run_observer_protocol(input, arm, &ProtocolCancellation::default())
        .map_err(|error| error.to_string())?;
    serde_json::to_writer(std::io::stdout().lock(), &result)
        .map_err(|error| format!("write observer protocol result: {error}"))?;
    std::io::stdout()
        .flush()
        .map_err(|error| format!("flush observer protocol result: {error}"))
}

fn wait_for_start_signal() -> Result<(), String> {
    let mut bytes = Vec::with_capacity(START_SIGNAL_BYTES.len());
    std::io::stdin()
        .lock()
        .take(START_SIGNAL_BYTES.len() as u64 + 1)
        .read_to_end(&mut bytes)
        .map_err(|error| format!("read start signal: {error}"))?;
    if bytes != START_SIGNAL_BYTES {
        return Err("start signal has invalid bytes".to_owned());
    }
    Ok(())
}

fn usage() -> String {
    "usage: distributed-state-ab-observer \
     dry-run <manifest.json> <C|D> <success|exit|hang|malformed> | \
     fake-protocol <C|D>"
        .to_owned()
}
