#![forbid(unsafe_code)]

use std::io::{Read as _, Write as _};
use std::path::Path;
use std::process::ExitCode;

use state_backend_qual::mechanism_bundle::validate_mechanism_bundle_path;
use state_backend_qual::mechanism_mapping::{
    validate_mechanism_mapping, MAX_MECHANISM_MAPPING_BYTES,
};
use state_backend_qual::model_result::validate_model_result;
use state_backend_qual::{validate_profile, MAX_MODEL_RESULT_BYTES, MAX_PROFILE_BYTES, NOTICE};

fn main() -> ExitCode {
    println!("{NOTICE}");
    let _ = std::io::stdout().flush();

    let mut arguments = std::env::args_os();
    let _program = arguments.next();
    let Some(command) = arguments.next() else {
        return usage();
    };
    match command.to_str() {
        Some("validate-profile") => {
            let Some(path) = arguments.next() else {
                return usage();
            };
            if arguments.next().is_some() {
                return usage();
            }
            validate_profile_path(Path::new(&path))
        }
        Some("validate-model-result") => {
            let Some(profile_path) = arguments.next() else {
                return usage();
            };
            let Some(result_path) = arguments.next() else {
                return usage();
            };
            if arguments.next().is_some() {
                return usage();
            }
            validate_model_result_paths(Path::new(&profile_path), Path::new(&result_path))
        }
        Some("validate-mechanism-mapping") => {
            let Some(profile_path) = arguments.next() else {
                return usage();
            };
            let Some(mapping_path) = arguments.next() else {
                return usage();
            };
            if arguments.next().is_some() {
                return usage();
            }
            validate_mechanism_mapping_paths(Path::new(&profile_path), Path::new(&mapping_path))
        }
        Some("validate-mechanism-bundle") => {
            let Some(input_path) = arguments.next() else {
                return usage();
            };
            if arguments.next().is_some() {
                return usage();
            }
            validate_mechanism_bundle_input(Path::new(&input_path))
        }
        _ => usage(),
    }
}

fn validate_mechanism_bundle_input(input_path: &Path) -> ExitCode {
    match validate_mechanism_bundle_path(input_path) {
        Ok(summary) => {
            let fail_reasons = if summary.candidate_fail_reasons.is_empty() {
                "none".to_owned()
            } else {
                summary.candidate_fail_reasons.join(",")
            };
            let debt = summary
                .maximum_debt_bytes
                .map_or_else(|| "not_applicable".to_owned(), |value| value.to_string());
            let stalls = summary
                .stall_time_permille
                .map_or_else(|| "not_applicable".to_owned(), |value| value.to_string());
            println!(
                "VALID_INELIGIBLE_MECHANISM_BUNDLE bundle={} profile={} mapping={} candidate={} \
                 observation_state={} fail_reasons={} samples={} debt_bytes={} stall_permille={} device_ms={}",
                summary.bundle_id,
                summary.profile_id,
                summary.mapping_id,
                summary.candidate_id,
                summary.observation_state,
                fail_reasons,
                summary.common_resource_samples,
                debt,
                stalls,
                summary.target_device_io_maximum_ms
            );
            ExitCode::SUCCESS
        }
        Err(error) => {
            eprintln!("INVALID_MECHANISM_BUNDLE {error}");
            ExitCode::from(2)
        }
    }
}

fn validate_mechanism_mapping_paths(profile_path: &Path, mapping_path: &Path) -> ExitCode {
    let profile_bytes = match read_bounded(profile_path, MAX_PROFILE_BYTES) {
        Ok(bytes) => bytes,
        Err(error) => {
            eprintln!("INVALID_INPUT {}: {error}", profile_path.display());
            return ExitCode::from(2);
        }
    };
    let mapping_bytes = match read_bounded(mapping_path, MAX_MECHANISM_MAPPING_BYTES) {
        Ok(bytes) => bytes,
        Err(error) => {
            eprintln!("INVALID_INPUT {}: {error}", mapping_path.display());
            return ExitCode::from(2);
        }
    };

    match validate_mechanism_mapping(&profile_bytes, &mapping_bytes) {
        Ok(summary) => {
            println!(
                "VALID_INELIGIBLE_MECHANISM_MAPPING mapping={} candidate={} debt={} stalls={}",
                summary.mapping_id,
                summary.candidate_id,
                summary.background_maintenance_debt_kind,
                summary.engine_pressure_stalls_kind
            );
            ExitCode::SUCCESS
        }
        Err(error) => {
            eprintln!("INVALID_MECHANISM_MAPPING {error}");
            ExitCode::from(2)
        }
    }
}

fn validate_profile_path(path: &Path) -> ExitCode {
    let bytes = match read_bounded(path, MAX_PROFILE_BYTES) {
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

fn validate_model_result_paths(profile_path: &Path, result_path: &Path) -> ExitCode {
    let profile_bytes = match read_bounded(profile_path, MAX_PROFILE_BYTES) {
        Ok(bytes) => bytes,
        Err(error) => {
            eprintln!("INVALID_INPUT {}: {error}", profile_path.display());
            return ExitCode::from(2);
        }
    };
    let result_bytes = match read_bounded(result_path, MAX_MODEL_RESULT_BYTES) {
        Ok(bytes) => bytes,
        Err(error) => {
            eprintln!("INVALID_INPUT {}: {error}", result_path.display());
            return ExitCode::from(2);
        }
    };

    match validate_model_result(&profile_bytes, &result_bytes) {
        Ok(summary) => {
            println!(
                "VALID_INELIGIBLE_MODEL_RESULT profile={} requests={}",
                summary.profile_id, summary.requests
            );
            ExitCode::SUCCESS
        }
        Err(error) => {
            eprintln!("INVALID_MODEL_RESULT {error}");
            ExitCode::from(2)
        }
    }
}

fn read_bounded(path: &Path, maximum_bytes: usize) -> std::io::Result<Vec<u8>> {
    let file = std::fs::File::open(path)?;
    let mut bytes = Vec::new();
    file.take((maximum_bytes + 1) as u64)
        .read_to_end(&mut bytes)?;
    Ok(bytes)
}

fn usage() -> ExitCode {
    eprintln!(
        "usage: state-backend-qual validate-profile <profile-json-path>\n       \
         state-backend-qual validate-model-result <profile-json-path> <result-json-path>\n       \
         state-backend-qual validate-mechanism-mapping <profile-json-path> <mapping-json-path>\n       \
         state-backend-qual validate-mechanism-bundle <validation-input-json-path>"
    );
    ExitCode::from(64)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bounded_reader_stops_after_the_rejection_sentinel() {
        let path = std::env::temp_dir().join(format!(
            "state-backend-qual-reader-{}.json",
            std::process::id()
        ));
        std::fs::write(&path, vec![b' '; MAX_PROFILE_BYTES + 32]).unwrap();
        let bytes = read_bounded(&path, MAX_PROFILE_BYTES).unwrap();
        std::fs::remove_file(path).unwrap();

        assert_eq!(bytes.len(), MAX_PROFILE_BYTES + 1);
    }
}
