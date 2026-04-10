# Kafka Avro schema auto-discovery

LaminarDB can auto-populate a Kafka source's Arrow schema from a
Confluent Schema Registry at DDL time, so users don't have to redeclare
the Avro record shape in SQL.

## When it fires

Auto-discovery runs when **all four** conditions are met on a
`CREATE SOURCE` statement:

1. No columns are declared in the `CREATE SOURCE` statement.
2. A connector is specified (`FROM KAFKA (...)` or `WITH ('connector' = 'kafka', ...)`).
3. `format = 'avro'`.
4. `schema.registry.url` points at a reachable Schema Registry.

The source definition needs a concrete `topic` — `topic.pattern` cannot
auto-discover because the subject it would query is ambiguous when
multiple topics match.

## Example

```sql
CREATE SOURCE events
WITH (
  'connector'           = 'kafka',
  'bootstrap.servers'   = 'broker1:9092,broker2:9092',
  'topic'               = 'orders',
  'group.id'            = 'laminardb',
  'format'              = 'avro',
  'schema.registry.url' = 'http://sr:8081'
);
```

At DDL time, LaminarDB will:

1. Create a throwaway factory instance of the Kafka source connector.
2. Call the connector's `discover_schema` with the `WITH (...)` properties.
3. The Kafka source fetches `{topic}-value` from the Schema Registry,
   converts the Avro schema to Arrow via `arrow-avro`, and reports it.
4. LaminarDB injects the resulting Arrow schema directly into the
   catalog — bypassing the SQL string round-trip, so complex types
   (`Map`, `List`, `Struct`, `Decimal(p, s)`) survive intact.

## Supported type round-trips

| Avro type                  | Arrow type                       |
|----------------------------|----------------------------------|
| `boolean`                  | `Boolean`                        |
| `int`                      | `Int32`                          |
| `long`                     | `Int64`                          |
| `float`                    | `Float32`                        |
| `double`                   | `Float64`                        |
| `string`                   | `Utf8`                           |
| `bytes`                    | `Binary`                         |
| `array<T>`                 | `List<T>`                        |
| `map<string, T>`           | `Map<Utf8, T>`                   |
| `record { ... }`           | `Struct { ... }`                 |
| `["null", T]`              | Nullable `T`                     |
| `decimal(p, s)` logical    | `Decimal128(p, s)`               |
| `date` logical             | `Date32`                         |
| `timestamp-millis/micros`  | `Timestamp(TimeUnit, None)`      |

Nested records and nested maps **are** supported via auto-discovery
because the path skips the SQL parser entirely.

## Subject name strategies

The default is Confluent's `TopicNameStrategy` (`{topic}-value`). For
non-default strategies, set `schema.registry.subject.name.strategy`:

| Strategy              | Subject format               | Requires                             |
|-----------------------|------------------------------|--------------------------------------|
| `topic-name` (default)| `{topic}-value`              | —                                    |
| `record-name`         | `{record_name}-value`        | `schema.registry.record.name`        |
| `topic-record-name`   | `{topic}-{record_name}-value`| `schema.registry.record.name`        |

The `schema.registry.record.name` requirement is enforced at DDL parse
time — misconfiguration surfaces at `CREATE SOURCE`, not at first
message decode.

## TLS / mTLS / basic auth

All of the existing Kafka source's Schema Registry connection options
apply during discovery — there is no separate configuration path:

| Option                                       | Effect                         |
|----------------------------------------------|--------------------------------|
| `schema.registry.username` / `password`      | HTTP basic auth                |
| `schema.registry.ssl.ca.location`            | TLS with CA bundle             |
| `schema.registry.ssl.certificate.location`   | mTLS client cert               |
| `schema.registry.ssl.key.location`           | mTLS client key                |

## Tuning

| Option                                       | Default | Effect                                                          |
|----------------------------------------------|---------|-----------------------------------------------------------------|
| `schema.registry.discovery.timeout.ms`       | `10000` | Hard upper bound on a single SR lookup during discovery.        |

Cross-region Schema Registry deployments should raise this past the
p99 SR latency; a wedged registry otherwise blocks `CREATE SOURCE` for
the full timeout.

## Failure modes

Discovery fails **closed** — it never silently substitutes an empty or
partial schema:

- **SR unreachable / HTTP error / parse error:** warning logged,
  schema left empty, the `CREATE SOURCE` errors with a clear message
  telling the operator to declare columns explicitly or verify SR
  connectivity. Nothing is registered in the catalog.
- **`topic.pattern` subscription:** warning logged, discovery skipped.
  Operator must declare columns explicitly.
- **Non-Avro format:** discovery skipped silently (only Avro has a
  Schema Registry subject path).

Discovery-outcome metrics are exposed on the Kafka source's
`ConnectorMetrics` under the custom keys
`kafka.sr_discovery_successes`, `kafka.sr_discovery_failures`, and
`kafka.sr_discovery_timeouts`.

## Operational behavior

### Startup dependency on Schema Registry

The catalog is **in-memory only** — it is rebuilt from DDL on every
server start. For sources declared without columns in TOML, that
means discovery runs on **every restart**, so the Schema Registry
must be reachable at startup or CREATE SOURCE fails with a clear
error and the server does not come up.

If your operational posture requires LaminarDB to start independently
of Schema Registry availability, declare columns explicitly in TOML
for the critical sources. The declared columns act as a pin and
auto-discovery is skipped.

### Schema drift after DDL

Discovery runs **once**, at `CREATE SOURCE` time. If the Schema
Registry subject evolves after that (new fields added, field types
changed), the catalog's view of the source is frozen at the version
captured during DDL — DataFusion query plans continue to use that
version.

At `open()` time the Kafka source re-fetches the current SR schema,
compares it against the catalog-baked schema, and logs a warning with
`missing_in_sr` / `added_in_sr` field lists if they diverge. The
operator's remediation is to re-apply the `CREATE SOURCE` DDL (which
re-runs discovery) so the catalog picks up the new shape.

For incompatible evolutions mid-stream, the
`schema.evolution.strategy` setting (`log` | `reject` | `ignore`)
controls the runtime deserializer's behavior; that is independent of
the DDL-time discovery path.

## Scope

- **Avro Schema Registry**: fully supported via `arrow-avro`.
- **JSON Schema and Protobuf subjects**: not supported — the
  dispatcher recognizes both and returns an actionable error
  rather than mis-parsing as Avro. Follow-up work is blocked on a
  maintained Rust crate for JSON Schema→Arrow and on wiring
  `prost-reflect` for Protobuf descriptors.
- **Hand-declared SQL columns** are primitives only
  (INT/BIGINT/VARCHAR/DOUBLE/DECIMAL/DATE/TIME/TIMESTAMP/ARRAY<T>).
  Nested types (maps, structs, records) must come from auto-discovery.

## Known limitations

- **`WATERMARK FOR` + auto-discovery** in the same DDL is not
  supported — the watermark column is validated against declared
  columns before discovery runs. Use the connector's
  `event.time.column` option instead, which composes with discovery.
- **Runtime SQL `CREATE SOURCE` does not survive restart.** For
  durable sources, declare them in TOML config. (Pre-existing
  LaminarDB property, not specific to auto-discovery.)
- **Key subject discovery is not implemented.** Only the Avro
  *value* subject is discovered.
- **Multi-topic subscriptions use the first topic's schema** and
  log a warning. Prefer one source per topic.
