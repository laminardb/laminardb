//! Command-line parsing and fail-closed validation for the standalone probe.

use super::*;

#[derive(Debug, Eq, PartialEq)]
pub(super) struct Cli {
    pub(super) brokers: String,
    pub(super) run_id: Option<String>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum AmbiguityKind {
    Marker,
    Data,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum AmbiguityOutcome {
    Applied,
    Unapplied,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) struct AmbiguityScenario {
    pub(super) kind: AmbiguityKind,
    pub(super) outcome: AmbiguityOutcome,
}

#[derive(Debug, Eq, PartialEq)]
pub(super) struct AmbiguityCli {
    pub(super) brokers: String,
    pub(super) proxy_listen: SocketAddr,
    pub(super) proxy_upstream: SocketAddr,
    pub(super) run_id: String,
    pub(super) scenario: AmbiguityScenario,
}

pub(super) enum Command {
    Run(Cli),
    RunAmbiguity(AmbiguityCli),
    Help,
}

pub(super) fn print_usage() {
    println!(
        "usage:\n  kafka-transaction-probe --brokers <host:port[,host:port...]> [--run-id <safe-label>]\n  kafka-transaction-probe --brokers <loopback-host:proxy-port> --run-id <safe-label> --ambiguity <marker-applied|marker-unapplied|data-applied|data-unapplied> --proxy-upstream <loopback-host:broker-port>"
    );
}

pub(super) fn parse_cli(arguments: impl IntoIterator<Item = String>) -> ProbeResult<Command> {
    let arguments = arguments.into_iter().collect::<Vec<_>>();
    if arguments.as_slice() == ["--help"] || arguments.as_slice() == ["-h"] {
        return Ok(Command::Help);
    }

    let mut brokers = None;
    let mut run_id = None;
    let mut ambiguity = None;
    let mut proxy_upstream = None;
    let mut index = 0;
    while index < arguments.len() {
        let flag = &arguments[index];
        let value = arguments
            .get(index + 1)
            .ok_or_else(|| format!("{flag} requires a value"))?;
        match flag.as_str() {
            "--brokers" if brokers.is_none() => brokers = Some(validate_brokers(value)?),
            "--run-id" if run_id.is_none() => run_id = Some(validate_run_id(value)?),
            "--ambiguity" if ambiguity.is_none() => {
                ambiguity = Some(parse_ambiguity_scenario(value)?)
            }
            "--proxy-upstream" if proxy_upstream.is_none() => {
                proxy_upstream = Some(validate_loopback_socket("--proxy-upstream", value)?)
            }
            "--brokers" | "--run-id" | "--ambiguity" | "--proxy-upstream" => {
                return Err(format!("duplicate option {flag}"));
            }
            _ => return Err(format!("unknown option {flag}")),
        }
        index += 2;
    }

    let brokers = brokers.ok_or_else(|| "--brokers is required".to_owned())?;
    match (ambiguity, proxy_upstream) {
        (None, None) => Ok(Command::Run(Cli { brokers, run_id })),
        (Some(scenario), Some(proxy_upstream)) => {
            let run_id = run_id.ok_or_else(|| "ambiguity mode requires --run-id".to_owned())?;
            let proxy_listen = validate_loopback_socket("--brokers", &brokers)?;
            if proxy_listen == proxy_upstream {
                return Err("proxy listen and upstream endpoints must differ".to_owned());
            }
            Ok(Command::RunAmbiguity(AmbiguityCli {
                brokers,
                proxy_listen,
                proxy_upstream,
                run_id,
                scenario,
            }))
        }
        (Some(_), None) => Err("ambiguity mode requires --proxy-upstream".to_owned()),
        (None, Some(_)) => Err("--proxy-upstream requires --ambiguity".to_owned()),
    }
}

fn parse_ambiguity_scenario(value: &str) -> ProbeResult<AmbiguityScenario> {
    match value {
        "marker-applied" => Ok(AmbiguityScenario {
            kind: AmbiguityKind::Marker,
            outcome: AmbiguityOutcome::Applied,
        }),
        "marker-unapplied" => Ok(AmbiguityScenario {
            kind: AmbiguityKind::Marker,
            outcome: AmbiguityOutcome::Unapplied,
        }),
        "data-applied" => Ok(AmbiguityScenario {
            kind: AmbiguityKind::Data,
            outcome: AmbiguityOutcome::Applied,
        }),
        "data-unapplied" => Ok(AmbiguityScenario {
            kind: AmbiguityKind::Data,
            outcome: AmbiguityOutcome::Unapplied,
        }),
        _ => Err(
            "--ambiguity must be marker-applied, marker-unapplied, data-applied, or data-unapplied"
                .to_owned(),
        ),
    }
}

fn validate_loopback_socket(field: &str, value: &str) -> ProbeResult<SocketAddr> {
    let address = value
        .parse::<SocketAddr>()
        .map_err(|error| format!("{field} must be one numeric socket address: {error}"))?;
    if !address.ip().is_loopback() || address.port() == 0 {
        return Err(format!("{field} must be a nonzero loopback socket address"));
    }
    Ok(address)
}

fn validate_brokers(value: &str) -> ProbeResult<String> {
    if value.is_empty() || value.len() > 1_024 {
        return Err("--brokers must contain 1..=1024 bytes".to_owned());
    }
    if value.trim() != value
        || value
            .bytes()
            .any(|byte| byte == 0 || byte.is_ascii_control())
    {
        return Err("--brokers contains whitespace at an edge, NUL, or a control byte".to_owned());
    }
    Ok(value.to_owned())
}

pub(super) fn validate_run_id(value: &str) -> ProbeResult<String> {
    if value.is_empty() || value.len() > 48 {
        return Err("--run-id must contain 1..=48 bytes".to_owned());
    }
    if !value
        .bytes()
        .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b'-'))
    {
        return Err("--run-id accepts only ASCII letters, digits, '.', '_', and '-'".to_owned());
    }
    Ok(value.to_owned())
}
