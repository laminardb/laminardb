use rdkafka::error::{KafkaError, RDKafkaErrorCode};

use crate::error::ConnectorError;

pub(super) fn fetch_error(topic: &str, error: &KafkaError) -> ConnectorError {
    classify(topic, error.rdkafka_error_code(), error)
}

pub(super) fn topic_error(topic: &str, code: RDKafkaErrorCode) -> ConnectorError {
    classify(topic, Some(code), code)
}

pub(super) fn invalid_response(topic: &str, detail: impl std::fmt::Display) -> ConnectorError {
    classify(topic, None, detail)
}

fn classify(
    topic: &str,
    code: Option<RDKafkaErrorCode>,
    detail: impl std::fmt::Display,
) -> ConnectorError {
    let message = format!("Kafka metadata lookup for topic '{topic}' failed: {detail}");
    if code.is_some_and(metadata_code_is_transient) {
        ConnectorError::ConnectionFailed(message)
    } else {
        ConnectorError::ConfigurationError(message)
    }
}

/// Metadata discovery retries only failures that can make progress without a
/// configuration or catalog change. Unknown and newly introduced codes fail
/// closed; notably, an unknown topic is not treated as eventual auto-creation.
fn metadata_code_is_transient(code: RDKafkaErrorCode) -> bool {
    matches!(
        code,
        RDKafkaErrorCode::BrokerDestroy
            | RDKafkaErrorCode::BrokerTransportFailure
            | RDKafkaErrorCode::Resolve
            | RDKafkaErrorCode::AllBrokersDown
            | RDKafkaErrorCode::OperationTimedOut
            | RDKafkaErrorCode::NodeUpdate
            | RDKafkaErrorCode::InProgress
            | RDKafkaErrorCode::PreviousInProgress
            | RDKafkaErrorCode::Outdated
            | RDKafkaErrorCode::TimedOutQueue
            | RDKafkaErrorCode::WaitCache
            | RDKafkaErrorCode::Interrupted
            | RDKafkaErrorCode::Partial
            | RDKafkaErrorCode::Retry
            | RDKafkaErrorCode::UnknownBroker
            | RDKafkaErrorCode::DestroyBroker
            | RDKafkaErrorCode::LeaderNotAvailable
            | RDKafkaErrorCode::NotLeaderForPartition
            | RDKafkaErrorCode::RequestTimedOut
            | RDKafkaErrorCode::BrokerNotAvailable
            | RDKafkaErrorCode::ReplicaNotAvailable
            | RDKafkaErrorCode::StaleControllerEpoch
            | RDKafkaErrorCode::NetworkException
            | RDKafkaErrorCode::NotController
            | RDKafkaErrorCode::KafkaStorageError
            | RDKafkaErrorCode::ReassignmentInProgress
            | RDKafkaErrorCode::FencedLeaderEpoch
            | RDKafkaErrorCode::UnknownLeaderEpoch
            | RDKafkaErrorCode::StaleBrokerEpoch
            | RDKafkaErrorCode::PreferredLeaderNotAvailable
            | RDKafkaErrorCode::EligibleLeadersNotAvailable
            | RDKafkaErrorCode::ThrottlingQuotaExceeded
            | RDKafkaErrorCode::RebootstrapRequired
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn network_and_leader_churn_are_transient() {
        for code in [
            RDKafkaErrorCode::BrokerTransportFailure,
            RDKafkaErrorCode::AllBrokersDown,
            RDKafkaErrorCode::OperationTimedOut,
            RDKafkaErrorCode::LeaderNotAvailable,
            RDKafkaErrorCode::RequestTimedOut,
            RDKafkaErrorCode::NetworkException,
            RDKafkaErrorCode::NotController,
            RDKafkaErrorCode::ThrottlingQuotaExceeded,
            RDKafkaErrorCode::RebootstrapRequired,
        ] {
            let error = fetch_error("orders", &KafkaError::MetadataFetch(code));
            assert!(
                matches!(error, ConnectorError::ConnectionFailed(_)),
                "metadata code {code:?}"
            );
            assert!(error.is_transient(), "metadata code {code:?}");
        }
    }

    #[test]
    fn credentials_topic_and_configuration_failures_are_terminal() {
        for code in [
            RDKafkaErrorCode::Authentication,
            RDKafkaErrorCode::SaslAuthenticationFailed,
            RDKafkaErrorCode::TopicAuthorizationFailed,
            RDKafkaErrorCode::ClusterAuthorizationFailed,
            RDKafkaErrorCode::InvalidTopic,
            RDKafkaErrorCode::InvalidConfig,
            RDKafkaErrorCode::UnsupportedSASLMechanism,
            RDKafkaErrorCode::UnknownTopic,
            RDKafkaErrorCode::UnknownTopicOrPartition,
            RDKafkaErrorCode::UnknownTopicId,
            RDKafkaErrorCode::Unknown,
        ] {
            let error = topic_error("orders", code);
            assert!(
                matches!(error, ConnectorError::ConfigurationError(_)),
                "metadata code {code:?}"
            );
            assert!(!error.is_transient(), "metadata code {code:?}");
        }
    }

    #[test]
    fn code_less_and_malformed_responses_fail_closed() {
        let fetch = fetch_error("orders", &KafkaError::ClientCreation("invalid TLS".into()));
        assert!(matches!(fetch, ConnectorError::ConfigurationError(_)));
        assert!(!fetch.is_transient());

        let response = invalid_response("orders", "metadata omitted the requested topic");
        assert!(matches!(response, ConnectorError::ConfigurationError(_)));
        assert!(!response.is_transient());
    }
}
