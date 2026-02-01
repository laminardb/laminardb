-- Kafka sinks for the Market Data demo.
-- Writes analytics results to Redpanda/Kafka topics.

CREATE SINK ohlc_output FROM ohlc_bars
INTO KAFKA (
    brokers = '${KAFKA_BROKERS}',
    topic = 'ohlc-bars',
    format = 'json'
);

CREATE SINK volume_output FROM volume_metrics
INTO KAFKA (
    brokers = '${KAFKA_BROKERS}',
    topic = 'volume-metrics',
    format = 'json'
);

CREATE SINK anomaly_output FROM anomaly_alerts
INTO KAFKA (
    brokers = '${KAFKA_BROKERS}',
    topic = 'anomaly-alerts',
    format = 'json'
);
