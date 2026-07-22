#![forbid(unsafe_code)]

use std::io::Write as _;
use std::path::Path;
use std::process::ExitCode;

use independent_soak_contract::{validate_contract, verify_oracle_fixture, NOTICE};

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
    if arguments.next().is_some() {
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
    match command.to_str() {
        Some("validate-contract") => match validate_contract(&bytes) {
            Ok(summary) => {
                println!(
                    "VALID_INELIGIBLE_DRAFT schema={} unresolved_gates={}",
                    summary.schema_version, summary.unresolved_gates
                );
                ExitCode::SUCCESS
            }
            Err(error) => {
                eprintln!("INVALID_DRAFT {error}");
                ExitCode::from(2)
            }
        },
        Some("verify-oracle-fixture") => match verify_oracle_fixture(&bytes) {
            Ok(summary) => {
                println!(
                    "ORACLE_FIXTURE_OK cases={} model_matches={} product_failures={} invalid_runs={}",
                    summary.cases,
                    summary.model_matches,
                    summary.product_failures,
                    summary.invalid_runs
                );
                ExitCode::SUCCESS
            }
            Err(error) => {
                eprintln!("INVALID_ORACLE_FIXTURE {error}");
                ExitCode::from(2)
            }
        },
        _ => usage(),
    }
}

fn usage() -> ExitCode {
    eprintln!(
        "usage: independent-soak-contract \
         <validate-contract|verify-oracle-fixture> <json-path>"
    );
    ExitCode::from(64)
}
