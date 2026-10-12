//! Bounded reads of broker-side topic configuration used to validate a connector's retention
//! contract before it relies on it.

use std::collections::HashMap;
use std::time::Duration;

use rdkafka::admin::{AdminClient, AdminOptions, ResourceSpecifier};
use rdkafka::client::DefaultClientContext;
use rdkafka::types::RDKafkaErrorCode;
use rdkafka::ClientConfig;

use crate::error::ConnectorError;

/// Read the effective configuration of `topic`.
///
/// # Errors
/// Returns a configuration error when the topic is missing or the principal lacks
/// `DescribeConfigs` permission on it, and a (retryable) connection error otherwise.
pub(crate) async fn describe_topic(
    connection: &ClientConfig,
    topic: &str,
    timeout: Duration,
) -> Result<HashMap<String, String>, ConnectorError> {
    let admin: AdminClient<DefaultClientContext> = connection.create().map_err(|error| {
        ConnectorError::ConnectionFailed(format!("create Kafka admin client: {error}"))
    })?;
    let options = AdminOptions::new().request_timeout(Some(timeout));
    let described = admin
        .describe_configs([&ResourceSpecifier::Topic(topic)], &options)
        .await;
    // The admin client owns librdkafka threads; drop it off the async worker.
    drop(tokio::task::spawn_blocking(move || drop(admin)));
    let resources = described.map_err(|error| {
        ConnectorError::ConnectionFailed(format!(
            "describe Kafka topic '{topic}' configuration: {error}"
        ))
    })?;
    let resource = resources
        .into_iter()
        .next()
        .ok_or_else(|| {
            ConnectorError::ConnectionFailed(format!(
                "Kafka returned no configuration for topic '{topic}'"
            ))
        })?
        .map_err(|code| match code {
            RDKafkaErrorCode::UnknownTopicOrPartition
            | RDKafkaErrorCode::TopicAuthorizationFailed
            | RDKafkaErrorCode::ClusterAuthorizationFailed => {
                ConnectorError::ConfigurationError(format!(
                    "cannot describe Kafka topic '{topic}' configuration: {code} (the client \
                     needs DescribeConfigs permission on the topic)"
                ))
            }
            _ => ConnectorError::ConnectionFailed(format!(
                "describe Kafka topic '{topic}' configuration: {code}"
            )),
        })?;
    Ok(resource
        .entries
        .into_iter()
        .filter_map(|entry| entry.value.map(|value| (entry.name, value)))
        .collect())
}

/// Whether `cleanup.policy` includes `compact`, so the broker may remove superseded records.
pub(crate) fn compacts(config: &HashMap<String, String>) -> bool {
    config
        .get("cleanup.policy")
        .is_some_and(|policy| policy.split(',').any(|item| item.trim() == "compact"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn compaction_is_detected_in_any_policy_list() {
        let policy =
            |value: &str| HashMap::from([("cleanup.policy".to_string(), value.to_string())]);
        assert!(compacts(&policy("delete, compact")));
        assert!(!compacts(&policy("delete")));
        assert!(!compacts(&HashMap::new()));
    }
}
