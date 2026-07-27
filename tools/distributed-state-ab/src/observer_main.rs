#![forbid(unsafe_code)]

use std::io::{Read as _, Write as _};
use std::path::PathBuf;
use std::process::ExitCode;
use std::str::FromStr as _;
use std::sync::mpsc::{self, RecvTimeoutError};
use std::time::Duration;

use distributed_state_ab::observer_protocol::{
    run_observer_protocol, ProtocolCancellation, ProtocolDispositionV2, ProvisionedObserverInput,
    SupervisorBootstrapSource, SUPERVISOR_CANCEL_BYTES,
};
use distributed_state_ab::{
    build_observer_result, seal_base_plan, validate_manifest_path, verify_current_executable, Arm,
    ObserverBehavior, NOTICE, START_SIGNAL_BYTES,
};

fn main() -> ExitCode {
    let failure_marker =
        if std::env::args_os().nth(1).as_deref() == Some(std::ffi::OsStr::new("fake-protocol")) {
            "INVALID_OBSERVER_FAKE_PROTOCOL"
        } else {
            "INVALID_OBSERVER_DRY_RUN"
        };
    match run() {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("{NOTICE}: {failure_marker} {error}");
            ExitCode::from(2)
        }
    }
}

fn run() -> Result<(), String> {
    let mut arguments = std::env::args_os();
    let _program = arguments.next();
    let command = arguments.next().ok_or_else(usage)?;
    validate_observer_environment()?;
    match command.to_str() {
        Some("dry-run") => run_dry_run(arguments),
        Some("fake-protocol") => run_fake_protocol(arguments),
        _ => Err(usage()),
    }
}

fn validate_observer_environment() -> Result<(), String> {
    for (name, value) in std::env::vars_os() {
        #[cfg(windows)]
        if name
            .to_str()
            .is_some_and(|name| name.eq_ignore_ascii_case("SystemRoot"))
            && !value.is_empty()
            && std::path::Path::new(&value).is_absolute()
        {
            continue;
        }
        #[cfg(not(windows))]
        let _ = (&name, &value);
        return Err("observer environment contains an unsupported entry".to_owned());
    }
    Ok(())
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
    let input = receive_supervisor_bootstrap()?;
    let cancellation = ProtocolCancellation::default();
    spawn_cancellation_listener(cancellation.clone())?;
    let result =
        run_observer_protocol(input, arm, &cancellation).map_err(|error| error.to_string())?;
    serde_json::to_writer(std::io::stdout().lock(), &result)
        .map_err(|error| format!("write observer protocol result: {error}"))?;
    std::io::stdout()
        .flush()
        .map_err(|error| format!("flush observer protocol result: {error}"))?;
    if result.disposition == ProtocolDispositionV2::Incomplete {
        return Err("observer protocol collection was incomplete".to_owned());
    }
    Ok(())
}

fn receive_supervisor_bootstrap() -> Result<ProvisionedObserverInput, String> {
    const BOOTSTRAP_TIMEOUT: Duration = Duration::from_secs(2);
    let (sender, receiver) = mpsc::sync_channel(1);
    std::thread::Builder::new()
        .name("observer-bootstrap".to_owned())
        .spawn(move || {
            let stdin = std::io::stdin();
            let mut source = SupervisorBootstrapSource::new(stdin.lock());
            let _ignored = sender.send(source.take());
        })
        .map_err(|_| "start supervisor bootstrap reader".to_owned())?;
    match receiver.recv_timeout(BOOTSTRAP_TIMEOUT) {
        Ok(result) => result.map_err(|error| error.to_string()),
        Err(RecvTimeoutError::Timeout) => Err("supervisor bootstrap deadline exceeded".to_owned()),
        Err(RecvTimeoutError::Disconnected) => {
            Err("supervisor bootstrap reader terminated".to_owned())
        }
    }
}

fn spawn_cancellation_listener(cancellation: ProtocolCancellation) -> Result<(), String> {
    std::thread::Builder::new()
        .name("observer-cancellation".to_owned())
        .spawn(move || {
            let stdin = std::io::stdin();
            let mut reader = stdin.lock();
            let mut control = [0_u8; SUPERVISOR_CANCEL_BYTES.len()];
            let mut observed = 0_usize;
            while observed < control.len() {
                match reader.read(&mut control[observed..]) {
                    Ok(0) if observed == 0 => return,
                    Ok(0) => {
                        cancellation.cancel();
                        return;
                    }
                    Ok(read) => observed += read,
                    Err(error) if error.kind() == std::io::ErrorKind::Interrupted => {}
                    Err(_) => {
                        cancellation.cancel();
                        return;
                    }
                }
            }
            // The documented frame cancels; any same-length invalid control also cancels fail-closed.
            cancellation.cancel();
        })
        .map(|_handle| ())
        .map_err(|_| "start supervisor cancellation reader".to_owned())
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
