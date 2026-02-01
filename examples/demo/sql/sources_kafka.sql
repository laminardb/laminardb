-- Kafka sources for the Market Data demo.
-- Reads from Redpanda/Kafka topics.

CREATE SOURCE market_ticks (
    symbol          VARCHAR NOT NULL,
    price           DOUBLE NOT NULL,
    bid             DOUBLE NOT NULL,
    ask             DOUBLE NOT NULL,
    volume          BIGINT NOT NULL,
    side            VARCHAR NOT NULL,
    ts              BIGINT NOT NULL
) FROM KAFKA (
    brokers = '${KAFKA_BROKERS}',
    topic = 'market-ticks',
    group_id = '${GROUP_ID}',
    format = 'json',
    offset_reset = 'earliest'
);

CREATE SOURCE order_events (
    order_id        VARCHAR NOT NULL,
    symbol          VARCHAR NOT NULL,
    side            VARCHAR NOT NULL,
    quantity        BIGINT NOT NULL,
    price           DOUBLE NOT NULL,
    ts              BIGINT NOT NULL
) FROM KAFKA (
    brokers = '${KAFKA_BROKERS}',
    topic = 'order-events',
    group_id = '${GROUP_ID}',
    format = 'json',
    offset_reset = 'earliest'
);
