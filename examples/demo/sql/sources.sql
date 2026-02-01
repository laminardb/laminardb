-- Streaming sources for the Market Data demo.
-- In embedded mode these are in-memory sources fed by the data generator.

CREATE SOURCE market_ticks (
    symbol          VARCHAR NOT NULL,
    price           DOUBLE NOT NULL,
    bid             DOUBLE NOT NULL,
    ask             DOUBLE NOT NULL,
    volume          BIGINT NOT NULL,
    side            VARCHAR NOT NULL,
    ts              BIGINT NOT NULL
);

CREATE SOURCE order_events (
    order_id        VARCHAR NOT NULL,
    symbol          VARCHAR NOT NULL,
    side            VARCHAR NOT NULL,
    quantity        BIGINT NOT NULL,
    price           DOUBLE NOT NULL,
    ts              BIGINT NOT NULL
);
