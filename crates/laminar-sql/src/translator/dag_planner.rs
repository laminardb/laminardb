//! DAG EXPLAIN formatter.
//!
//! Converts a `StreamingDag` into a human-readable topology description
//! suitable for `EXPLAIN DAG` output.

use std::fmt;
use std::fmt::Write;

use laminar_core::dag::{DagChannelType, StreamingDag};

/// Structured output from `EXPLAIN DAG`.
///
/// Contains both human-readable text and structured data for programmatic
/// consumption.
#[derive(Debug, Clone)]
pub struct DagExplainOutput {
    /// Number of nodes in the DAG.
    pub node_count: usize,
    /// Number of edges in the DAG.
    pub edge_count: usize,
    /// Source node names.
    pub sources: Vec<String>,
    /// Execution order as node names.
    pub execution_order: Vec<String>,
    /// Shared stages (nodes with fan-out > 1) and their consumer counts.
    pub shared_stages: Vec<(String, usize)>,
    /// Formatted topology text for display.
    pub topology_text: String,
}

impl fmt::Display for DagExplainOutput {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.topology_text)
    }
}

/// Formats a DAG topology into an `EXPLAIN DAG` output.
///
/// # Example output
///
/// ```text
/// DAG Topology (5 nodes, 4 edges)
/// ================================
/// Sources: [trades]
/// Execution order: trades -> ohlc_1s -> ohlc_1m -> vwap, anomaly
/// Shared stages: ohlc_1s (2 consumers)
/// Channel types:
///   trades -> ohlc_1s: SPSC
///   ohlc_1s -> vwap: SPMC
///   ohlc_1s -> anomaly: SPMC
/// ```
#[must_use]
pub fn format_dag_explain(dag: &StreamingDag) -> DagExplainOutput {
    let node_count = dag.node_count();
    let edge_count = dag.edge_count();

    // Source names
    let sources: Vec<String> = dag
        .sources()
        .iter()
        .filter_map(|&id| dag.node_name(id))
        .collect();

    // Execution order names
    let execution_order: Vec<String> = dag
        .execution_order()
        .iter()
        .filter_map(|&id| dag.node_name(id))
        .collect();

    // Shared stages
    let shared_stages: Vec<(String, usize)> = dag
        .shared_stages()
        .iter()
        .filter_map(|(id, meta)| dag.node_name(*id).map(|name| (name, meta.consumer_count)))
        .collect();

    // Build text output
    let mut text = String::new();
    let _ = writeln!(
        text,
        "DAG Topology ({node_count} nodes, {edge_count} edges)"
    );
    text.push_str(&"=".repeat(40));
    text.push('\n');

    // Sources line
    let _ = writeln!(text, "Sources: [{}]", sources.join(", "));

    // Execution order line
    let _ = writeln!(text, "Execution order: {}", execution_order.join(" -> "));

    // Shared stages
    if !shared_stages.is_empty() {
        let shared_strs: Vec<String> = shared_stages
            .iter()
            .map(|(name, count)| format!("{name} ({count} consumers)"))
            .collect();
        let _ = writeln!(text, "Shared stages: {}", shared_strs.join(", "));
    }

    // Channel types
    text.push_str("Channel types:\n");
    for edge in dag.edges().values() {
        let src_name = dag.node_name(edge.source).unwrap_or_default();
        let tgt_name = dag.node_name(edge.target).unwrap_or_default();
        let ch_type = match edge.channel_type {
            DagChannelType::Spsc => "SPSC",
            DagChannelType::Spmc => "SPMC",
            DagChannelType::Mpsc => "MPSC",
        };
        let _ = writeln!(text, "  {src_name} -> {tgt_name}: {ch_type}");
    }

    DagExplainOutput {
        node_count,
        edge_count,
        sources,
        execution_order,
        shared_stages,
        topology_text: text,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_schema::{DataType, Field, Schema};
    use laminar_core::dag::DagBuilder;
    use std::sync::Arc;

    fn int_schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![Field::new(
            "value",
            DataType::Int64,
            false,
        )]))
    }

    #[test]
    fn test_explain_dag_output() {
        let schema = int_schema();
        let dag = DagBuilder::new()
            .source("trades", schema.clone())
            .operator("ohlc_1s", schema.clone())
            .connect("trades", "ohlc_1s")
            .fan_out("ohlc_1s", |b| {
                b.branch("vwap", schema.clone())
                    .branch("anomaly", schema.clone())
            })
            .sink_for("vwap", "sink_vwap", schema.clone())
            .sink_for("anomaly", "sink_anomaly", schema.clone())
            .build()
            .unwrap();

        let output = format_dag_explain(&dag);

        // Structured data
        assert_eq!(output.node_count, 6);
        assert_eq!(output.edge_count, 5);
        assert_eq!(output.sources, vec!["trades".to_string()]);
        assert!(output.execution_order.contains(&"trades".to_string()));
        assert!(output.execution_order.contains(&"ohlc_1s".to_string()));
        assert!(output.execution_order.contains(&"vwap".to_string()));

        // Shared stage
        assert!(!output.shared_stages.is_empty());
        let shared = output
            .shared_stages
            .iter()
            .find(|(name, _)| name == "ohlc_1s");
        assert!(shared.is_some());
        assert_eq!(shared.unwrap().1, 2);

        // Text output
        assert!(output.topology_text.contains("DAG Topology"));
        assert!(output.topology_text.contains("6 nodes"));
        assert!(output.topology_text.contains("5 edges"));
        assert!(output.topology_text.contains("Sources: [trades]"));
        assert!(output.topology_text.contains("Channel types:"));
        assert!(output.topology_text.contains("SPMC"));

        // Display impl
        let display = format!("{output}");
        assert!(display.contains("DAG Topology"));
    }

    #[test]
    fn test_explain_dag_linear() {
        let schema = int_schema();
        let dag = DagBuilder::new()
            .source("src", schema.clone())
            .operator("op", schema.clone())
            .sink_for("op", "snk", schema.clone())
            .connect("src", "op")
            .build()
            .unwrap();

        let output = format_dag_explain(&dag);
        assert_eq!(output.node_count, 3);
        assert_eq!(output.edge_count, 2);
        assert!(output.shared_stages.is_empty());
        assert!(output.topology_text.contains("SPSC"));
    }
}
