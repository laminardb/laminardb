//! One bounded HTTP hop for durable topology submission and explicit adoption.

use laminar_core::cluster::control::{LeaseError, TopologyError};
use serde::{de::DeserializeOwned, Serialize};

use super::{DbError, LaminarDB};

const MAX_FORWARDED_RESPONSE_BYTES: usize = 1024 * 1024;

#[cfg(test)]
mod tests;

#[derive(serde::Deserialize)]
struct ErrorBody {
    error: String,
    #[serde(default)]
    code: Option<String>,
}

impl LaminarDB {
    pub(super) async fn forward_topology_request<Q: Serialize + Sync, R: DeserializeOwned>(
        &self,
        address: &str,
        path: &str,
        request: &Q,
        deadline: tokio::time::Instant,
    ) -> Result<R, DbError> {
        let remaining = deadline
            .checked_duration_since(tokio::time::Instant::now())
            .filter(|remaining| !remaining.is_zero())
            .ok_or(TopologyError::Contended)?;
        let mut url = reqwest::Url::parse(&format!("http://{address}/")).map_err(|_| {
            TopologyError::Protocol("durable leader HTTP address is invalid".into())
        })?;
        if url.host_str().is_none()
            || url.port_or_known_default().is_none()
            || !url.username().is_empty()
            || url.password().is_some()
            || url.path() != "/"
            || url.query().is_some()
            || url.fragment().is_some()
        {
            return Err(TopologyError::Protocol(
                "durable leader must advertise one HTTP host and port".into(),
            )
            .into());
        }
        url.set_path(path);
        let client = reqwest::Client::builder()
            .connect_timeout(remaining)
            .timeout(remaining)
            .redirect(reqwest::redirect::Policy::none())
            .build()
            .map_err(|error| TopologyError::Authority(LeaseError::Io(error.to_string())))?;
        let mut post = client.post(url).json(request).header(
            "x-laminar-topology-budget-nanos",
            u64::try_from(remaining.as_nanos())
                .unwrap_or(u64::MAX)
                .to_string(),
        );
        if let Some(token) = &self.config.http_auth_token {
            post = post.bearer_auth(token.expose());
        }
        let mut response = post.send().await.map_err(|error| {
            if error.is_timeout() {
                TopologyError::Contended
            } else {
                TopologyError::Authority(LeaseError::Io(error.to_string()))
            }
        })?;
        let status = response.status();
        if response
            .content_length()
            .is_some_and(|length| length > MAX_FORWARDED_RESPONSE_BYTES as u64)
        {
            return Err(
                TopologyError::Invalid("leader topology response exceeds 1 MiB".into()).into(),
            );
        }
        let mut body = Vec::new();
        while let Some(chunk) = response.chunk().await.map_err(|error| {
            if error.is_timeout() {
                TopologyError::Contended
            } else {
                TopologyError::Authority(LeaseError::Io(error.to_string()))
            }
        })? {
            if chunk.len() > MAX_FORWARDED_RESPONSE_BYTES.saturating_sub(body.len()) {
                return Err(TopologyError::Invalid(
                    "leader topology response exceeds 1 MiB".into(),
                )
                .into());
            }
            body.extend_from_slice(&chunk);
        }
        if status.is_success() {
            return serde_json::from_slice(&body).map_err(|_| {
                TopologyError::Invalid("leader returned an invalid topology receipt".into()).into()
            });
        }
        let error = serde_json::from_slice::<ErrorBody>(&body).unwrap_or_else(|_| ErrorBody {
            error: format!("leader rejected topology request with HTTP {status}"),
            code: None,
        });
        if let Some(code) = error.code.as_deref() {
            use laminar_core::error_codes;
            if code == error_codes::TOPOLOGY_FENCED {
                return Err(TopologyError::Fenced.into());
            }
            if code == error_codes::TOPOLOGY_PROTOCOL_UNSUPPORTED {
                return Err(TopologyError::Protocol(error.error).into());
            }
            if code == error_codes::TOPOLOGY_AUTHORITY_FAILED {
                return Err(TopologyError::Authority(LeaseError::Io(error.error)).into());
            }
        }
        let message = error.error;
        Err(match status.as_u16() {
            400 => TopologyError::Invalid(message),
            409 => TopologyError::Conflict(message),
            422 => TopologyError::Unsupported(message),
            429 => TopologyError::PlanningBusy,
            504 => TopologyError::Contended,
            _ => TopologyError::Authority(LeaseError::Io(message)),
        }
        .into())
    }
}
