#![forbid(unsafe_code)]

use std::io::Write as _;
use std::path::Path;
use std::process::ExitCode;

use state_backend_qual::{validate_profile, NOTICE};

fn main() -> ExitCode {
    println!("{NOTICE}");
    let _ = std::io::stdout().flush();

    let mut arguments = std::env::args_os();
    let _program = arguments.next();
    let Some(command) = arguments.next() else {
        return usage();
    };
    let Some(path) = arguments.next() else {
        return usage();
    };
    if arguments.next().is_some() || command != "validate-profile" {
        return usage();
    }

    let path = Path::new(&path);
    let bytes = match std::fs::read(path) {
        Ok(bytes) => bytes,
        Err(error) => {
            eprintln!("INVALID_INPUT {}: {error}", path.display());
            return ExitCode::from(2);
        }
    };

    match validate_profile(&bytes) {
        Ok(summary) => {
            println!(
                "VALID_INELIGIBLE_PROFILE schema={} profile={} status={}",
                summary.schema_version, summary.profile_id, summary.status
            );
            ExitCode::SUCCESS
        }
        Err(error) => {
            eprintln!("INVALID_PROFILE {error}");
            ExitCode::from(2)
        }
    }
}

fn usage() -> ExitCode {
    eprintln!("usage: state-backend-qual validate-profile <json-path>");
    ExitCode::from(64)
}
