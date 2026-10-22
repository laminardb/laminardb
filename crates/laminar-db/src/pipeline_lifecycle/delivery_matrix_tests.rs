//! Source × sink delivery guarantee matrix derived from the pre-I/O admission functions.
//!
//! The rendered tables must match the generated block in `docs/DELIVERY_GUARANTEES.md`; run
//! with `LAMINAR_UPDATE_DELIVERY_MATRIX=1` to rewrite it after an intended admission change.

use std::fmt::Write as _;

use arrow_schema::{DataType, Field, Schema};
use laminar_connectors::config::ConnectorConfig;
use laminar_connectors::connector::DeliveryGuarantee;
use laminar_connectors::registry::ConnectorRegistry;
use laminar_core::checkpoint::object_store_builder::CheckpointStorageScope;

use super::{
    admit_sink, admit_source_recovery_contract, RuntimeMode, SinkAdmissionContext,
    CLUSTER_BEST_EFFORT,
};
use crate::pipeline_callback::encode_arrow_schema;
use crate::LaminarDB;

const MATRIX_DOC: &str = concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/../../docs/DELIVERY_GUARANTEES.md"
);
const BEGIN_MARKER: &str = "<!-- delivery-matrix:begin -->";
const END_MARKER: &str = "<!-- delivery-matrix:end -->";
const UPDATE_ENV: &str = "LAMINAR_UPDATE_DELIVERY_MATRIX";

const GUARANTEES: [DeliveryGuarantee; 3] = [
    DeliveryGuarantee::ExactlyOnce,
    DeliveryGuarantee::AtLeastOnce,
    DeliveryGuarantee::BestEffort,
];
const MODES: [RuntimeMode; 2] = [RuntimeMode::Local, RuntimeMode::Cluster];

type Properties = &'static [(&'static str, &'static str)];

/// One built-in connector configuration whose guarantees the matrix reports.
struct Variant {
    label: &'static str,
    connector: &'static str,
    properties: Properties,
    /// Properties a connector requires only when the pipeline runs exactly-once.
    exact_properties: Properties,
}

const SOURCES: &[Variant] = &[
    Variant {
        label: "Kafka",
        connector: "kafka",
        properties: &[
            ("bootstrap.servers", "localhost:9092"),
            ("topic", "events"),
            ("group.id", "matrix"),
            ("format", "json"),
        ],
        exact_properties: &[],
    },
    Variant {
        label: "Kafka (Debezium)",
        connector: "kafka",
        properties: &[
            ("bootstrap.servers", "localhost:9092"),
            ("topic", "events"),
            ("group.id", "matrix"),
            ("format", "debezium"),
        ],
        exact_properties: &[],
    },
    Variant {
        label: "PostgreSQL CDC",
        connector: "postgres-cdc",
        properties: &[
            ("host", "localhost"),
            ("database", "app"),
            ("slot.name", "matrix"),
            ("publication", "matrix"),
            ("table", "public.orders"),
            ("ssl.mode", "disable"),
        ],
        exact_properties: &[],
    },
    Variant {
        label: "MongoDB change stream",
        connector: "mongodb-cdc",
        properties: &[
            ("connection.uri", "mongodb://localhost:27017"),
            ("database", "app"),
            ("collection", "orders"),
        ],
        exact_properties: &[],
    },
    Variant {
        label: "Delta Lake change feed",
        connector: "delta-lake",
        properties: &[("table.path", "s3://lake/orders"), ("read.mode", "changes")],
        exact_properties: &[],
    },
    Variant {
        label: "Iceberg (snapshot read)",
        connector: "iceberg",
        properties: &[
            ("catalog.type", "rest"),
            ("catalog.uri", "http://catalog.invalid"),
            ("catalog.warehouse", "s3://warehouse/root"),
            ("namespace", "app"),
            ("table.name", "orders"),
        ],
        exact_properties: &[],
    },
    Variant {
        label: "Iceberg (append read)",
        connector: "iceberg",
        properties: &[
            ("catalog.type", "rest"),
            ("catalog.uri", "http://catalog.invalid"),
            ("catalog.warehouse", "s3://warehouse/root"),
            ("namespace", "app"),
            ("table.name", "orders"),
            ("read.mode", "append"),
        ],
        exact_properties: &[],
    },
    Variant {
        label: "Files",
        connector: "files",
        properties: &[("path", "/data/in/*.json"), ("format", "json")],
        exact_properties: &[],
    },
    Variant {
        label: "RabbitMQ",
        connector: "rabbitmq",
        properties: &[("url", "amqp://localhost:5672/%2f"), ("queue", "orders")],
        exact_properties: &[],
    },
    Variant {
        label: "IBM MQ",
        connector: "ibmmq",
        properties: &[("queue", "ORDERS")],
        exact_properties: &[],
    },
    Variant {
        label: "NATS JetStream",
        connector: "nats",
        properties: &[
            ("mode", "jetstream"),
            ("servers", "nats://localhost:4222"),
            ("subject", "orders"),
            ("stream", "ORDERS"),
        ],
        exact_properties: &[],
    },
    Variant {
        label: "WebSocket",
        connector: "websocket",
        properties: &[("url", "ws://localhost:8080/feed")],
        exact_properties: &[],
    },
    Variant {
        label: "OpenTelemetry",
        connector: "otel",
        properties: &[("bind.address", "127.0.0.1"), ("port", "4317")],
        exact_properties: &[],
    },
    Variant {
        label: "FIX TagValue",
        connector: "fix",
        properties: &[
            ("host", "127.0.0.1"),
            ("port", "9878"),
            ("transport", "tcp"),
            ("sender.comp.id", "LAMINAR"),
            ("target.comp.id", "VENUE"),
            ("dictionary.path", "FIX44.xml"),
            ("messages", "8"),
            ("journal.path", "journal"),
        ],
        exact_properties: &[],
    },
    Variant {
        label: "Generator",
        connector: "generator",
        properties: &[],
        exact_properties: &[],
    },
];

const SINKS: &[Variant] = &[
    Variant {
        label: "Kafka",
        connector: "kafka",
        properties: &[
            ("bootstrap.servers", "localhost:9092"),
            ("topic", "out"),
            ("format", "json"),
        ],
        exact_properties: &[("commit.topic", "laminar-commits")],
    },
    Variant {
        label: "Delta Lake append (S3/S3A)",
        connector: "delta-lake",
        properties: &[("table.path", "s3://lake/out"), ("write.mode", "append")],
        exact_properties: &[],
    },
    Variant {
        label: "Delta Lake append (Azure)",
        connector: "delta-lake",
        properties: &[
            ("table.path", "az://lake/out"),
            ("write.mode", "append"),
            ("storage.azure_storage_account_name", "matrix"),
        ],
        exact_properties: &[],
    },
    Variant {
        label: "Delta Lake upsert",
        connector: "delta-lake",
        properties: &[
            ("table.path", "s3://lake/out"),
            ("write.mode", "upsert"),
            ("merge.key.columns", "id"),
        ],
        exact_properties: &[],
    },
    Variant {
        label: "Iceberg append (REST + S3)",
        connector: "iceberg",
        properties: &[
            ("catalog.type", "rest"),
            ("catalog.uri", "http://catalog.invalid"),
            ("catalog.warehouse", "s3://warehouse/root"),
            ("storage.type", "s3"),
            ("storage.endpoint", "http://objects.invalid"),
            ("storage.region", "us-east-1"),
            ("namespace", "app"),
            ("table.name", "out"),
        ],
        exact_properties: &[],
    },
    Variant {
        label: "PostgreSQL",
        connector: "postgres-sink",
        properties: &[
            ("hostname", "localhost"),
            ("database", "app"),
            ("username", "writer"),
            ("table.name", "out"),
        ],
        exact_properties: &[],
    },
    Variant {
        label: "MongoDB",
        connector: "mongodb-sink",
        properties: &[
            ("connection.uri", "mongodb://localhost:27017"),
            ("database", "app"),
            ("collection", "out"),
        ],
        exact_properties: &[],
    },
    Variant {
        label: "Files",
        connector: "files",
        properties: &[("path", "/data/out"), ("format", "json")],
        exact_properties: &[],
    },
    Variant {
        label: "RabbitMQ",
        connector: "rabbitmq",
        properties: &[("url", "amqp://localhost:5672/%2f"), ("routing.key", "out")],
        exact_properties: &[],
    },
    Variant {
        label: "IBM MQ",
        connector: "ibmmq",
        properties: &[("queue", "OUT")],
        exact_properties: &[],
    },
    Variant {
        label: "NATS JetStream",
        connector: "nats",
        properties: &[
            ("mode", "jetstream"),
            ("servers", "nats://localhost:4222"),
            ("subject", "out"),
            ("stream", "OUT"),
        ],
        exact_properties: &[],
    },
    Variant {
        label: "WebSocket server",
        connector: "websocket",
        properties: &[("mode", "server"), ("bind.address", "127.0.0.1:8081")],
        exact_properties: &[],
    },
];

impl Variant {
    fn config(&self, delivery: DeliveryGuarantee) -> ConnectorConfig {
        let mut config = ConnectorConfig::new(self.connector);
        for (key, value) in self.properties {
            config.set(*key, *value);
        }
        if delivery == DeliveryGuarantee::ExactlyOnce {
            for (key, value) in self.exact_properties {
                config.set(*key, *value);
            }
        }
        config
    }

    /// Sink configuration as sink creation delivers it: with the input schema and the pipeline
    /// guarantee injected.
    fn sink_config(&self, delivery: DeliveryGuarantee) -> ConnectorConfig {
        let schema = Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("payload", DataType::Utf8, true),
        ]);
        let mut config = self.config(delivery);
        config.set("_arrow_schema", encode_arrow_schema(&schema));
        config.set("delivery.guarantee", delivery.to_string());
        config
    }
}

/// Guarantees admitted per runtime mode, strongest first.
type Admitted = [Vec<DeliveryGuarantee>; 2];

fn registry() -> ConnectorRegistry {
    let registry = ConnectorRegistry::new();
    LaminarDB::register_builtin_connectors(&registry).expect("register built-in connectors");
    registry
}

/// A connector may reject a guarantee while parsing its configuration; that is admission too.
/// A variant that admits nothing anywhere is a broken representative configuration instead.
fn admitted(
    variant: &Variant,
    config: impl Fn(&Variant, DeliveryGuarantee) -> ConnectorConfig,
    admit: impl Fn(&ConnectorConfig, DeliveryGuarantee, RuntimeMode) -> Result<(), String>,
) -> Admitted {
    let mut errors = Vec::new();
    let admitted = MODES.map(|runtime| {
        GUARANTEES
            .into_iter()
            .filter(
                |&delivery| match admit(&config(variant, delivery), delivery, runtime) {
                    Ok(()) => true,
                    Err(error) => {
                        errors.push(format!("{runtime:?} {delivery}: {error}"));
                        false
                    }
                },
            )
            .collect::<Vec<_>>()
    });
    assert!(
        admitted.iter().any(|guarantees| !guarantees.is_empty()),
        "'{}' ({}) admits no guarantee; its representative configuration is invalid:\n{}",
        variant.label,
        variant.connector,
        errors.join("\n")
    );
    admitted
}

fn admitted_source(registry: &ConnectorRegistry, variant: &Variant) -> Admitted {
    admitted(variant, Variant::config, |config, delivery, runtime| {
        let connector = registry
            .create_source(config, None)
            .map_err(|error| error.to_string())?;
        let contract = connector
            .contract(config)
            .map_err(|error| error.to_string())?;
        if runtime == RuntimeMode::Cluster && delivery == DeliveryGuarantee::BestEffort {
            return Err(CLUSTER_BEST_EFFORT.to_owned());
        }
        // Keyed inputs additionally need an admitted stateful SQL route; that is query shape,
        // not delivery, so only the recovery contract is evaluated here.
        admit_source_recovery_contract(contract, delivery, true, runtime).map_err(str::to_owned)
    })
}

fn admitted_sink(registry: &ConnectorRegistry, variant: &Variant) -> Admitted {
    admitted(
        variant,
        Variant::sink_config,
        |config, delivery, runtime| {
            let sink = registry
                .create_sink(config, None)
                .map_err(|error| error.to_string())?;
            admit_sink(
                sink.as_ref(),
                SinkAdmissionContext {
                    config,
                    name: "out",
                    input: "events",
                    delivery,
                    runtime,
                    carries_changelog: false,
                    mutation_key: None,
                    checkpointing_enabled: true,
                    checkpoint_storage_scope: CheckpointStorageScope::ClusterShared,
                },
            )
            .map(|_| ())
            .map_err(|error| error.to_string())
        },
    )
}

fn short(delivery: DeliveryGuarantee) -> &'static str {
    match delivery {
        DeliveryGuarantee::ExactlyOnce => "EO",
        DeliveryGuarantee::AtLeastOnce => "ALO",
        DeliveryGuarantee::BestEffort => "BE",
    }
}

fn strongest(source: &[DeliveryGuarantee], sink: &[DeliveryGuarantee]) -> &'static str {
    GUARANTEES
        .into_iter()
        .find(|delivery| source.contains(delivery) && sink.contains(delivery))
        .map_or("—", short)
}

fn render_capabilities(out: &mut String, title: &str, rows: &[(&Variant, Admitted)]) {
    let _ = writeln!(out, "| {title} | Embedded / single-node | Cluster |");
    let _ = writeln!(out, "|---|---|---|");
    for (variant, [local, cluster]) in rows {
        let list = |admitted: &Vec<DeliveryGuarantee>| {
            if admitted.is_empty() {
                "rejected".to_owned()
            } else {
                admitted
                    .iter()
                    .map(|&delivery| short(delivery))
                    .collect::<Vec<_>>()
                    .join(", ")
            }
        };
        let _ = writeln!(
            out,
            "| {} | {} | {} |",
            variant.label,
            list(local),
            list(cluster)
        );
    }
}

fn render_pairs(
    out: &mut String,
    mode: usize,
    sources: &[(&Variant, Admitted)],
    sinks: &[(&Variant, Admitted)],
) {
    let header = sinks
        .iter()
        .map(|(sink, _)| sink.label)
        .collect::<Vec<_>>()
        .join(" | ");
    let _ = writeln!(out, "| Source \\ Sink | {header} |");
    let _ = writeln!(out, "|---|{}", "---|".repeat(sinks.len()));
    for (source, source_admitted) in sources {
        let cells = sinks
            .iter()
            .map(|(_, sink_admitted)| strongest(&source_admitted[mode], &sink_admitted[mode]))
            .collect::<Vec<_>>()
            .join(" | ");
        let _ = writeln!(out, "| {} | {cells} |", source.label);
    }
}

fn render_matrix() -> String {
    let registry = registry();
    let sources = SOURCES
        .iter()
        .map(|variant| (variant, admitted_source(&registry, variant)))
        .collect::<Vec<_>>();
    let sinks = SINKS
        .iter()
        .map(|variant| (variant, admitted_sink(&registry, variant)))
        .collect::<Vec<_>>();
    let mut out = String::new();
    let _ = writeln!(out, "### Sources\n");
    render_capabilities(&mut out, "Source", &sources);
    let _ = writeln!(out, "\n### Sinks\n");
    render_capabilities(&mut out, "Sink", &sinks);
    for (mode, title) in [(0, "Embedded / single-node"), (1, "Cluster")] {
        let _ = writeln!(out, "\n### {title}: strongest guarantee per pair\n");
        render_pairs(&mut out, mode, &sources, &sinks);
    }
    out
}

#[test]
fn delivery_matrix_doc_matches_admission() {
    let rendered = render_matrix();
    let doc = std::fs::read_to_string(MATRIX_DOC).expect("read docs/DELIVERY_GUARANTEES.md");
    let begin = doc.find(BEGIN_MARKER).expect("matrix begin marker") + BEGIN_MARKER.len();
    let end = doc.find(END_MARKER).expect("matrix end marker");
    let current = doc[begin..end].trim();
    if current == rendered.trim() {
        return;
    }
    if std::env::var_os(UPDATE_ENV).is_some() {
        let updated = format!(
            "{}\n\n{}\n\n{}",
            &doc[..begin],
            rendered.trim(),
            &doc[end..]
        );
        std::fs::write(MATRIX_DOC, updated).expect("rewrite docs/DELIVERY_GUARANTEES.md");
        return;
    }
    panic!(
        "docs/DELIVERY_GUARANTEES.md is stale; rerun with {UPDATE_ENV}=1 after confirming the \
         admission change is intended. Rendered matrix:\n{rendered}"
    );
}
