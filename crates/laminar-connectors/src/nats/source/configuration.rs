use super::{err, map_ack_policy, pull, ConnectorError, DeliverPolicy, NatsSourceConfig};

pub(super) fn build_pull_config(
    cfg: &NatsSourceConfig,
    consumer_name: &str,
) -> Result<pull::Config, ConnectorError> {
    let filter_subjects = if cfg.subject_filters.is_empty() {
        cfg.subject.iter().cloned().collect()
    } else {
        cfg.subject_filters.clone()
    };

    Ok(pull::Config {
        durable_name: Some(consumer_name.to_string()),
        filter_subjects,
        deliver_policy: map_deliver_policy(cfg)?,
        ack_policy: map_ack_policy(cfg.ack_policy),
        ack_wait: cfg.ack_wait,
        max_deliver: cfg.max_deliver,
        max_ack_pending: cfg.max_ack_pending,
        ..Default::default()
    })
}

fn map_deliver_policy(
    cfg: &NatsSourceConfig,
) -> Result<async_nats::jetstream::consumer::DeliverPolicy, ConnectorError> {
    use async_nats::jetstream::consumer::DeliverPolicy as Nats;
    Ok(match cfg.deliver_policy {
        DeliverPolicy::All => Nats::All,
        DeliverPolicy::New => Nats::New,
        DeliverPolicy::ByStartSequence => Nats::ByStartSequence {
            start_sequence: cfg.start_sequence.unwrap_or(1),
        },
        DeliverPolicy::ByStartTime => {
            let raw = cfg
                .start_time
                .as_deref()
                .ok_or_else(|| err("deliver.policy=by_start_time requires 'start.time'"))?;
            let start_time =
                time::OffsetDateTime::parse(raw, &time::format_description::well_known::Rfc3339)
                    .map_err(|e| err(&format!("start.time '{raw}' is not valid RFC3339: {e}")))?;
            Nats::ByStartTime { start_time }
        }
    })
}
