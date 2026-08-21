//! Pipeline-topology rendering for the demo TUI.

use super::*;
use laminar_db::{PipelineNode, PipelineTopology};

// -- DAG View --

/// DAG pipeline view: renders the topology as a layered graph.
pub(super) fn draw_dag(f: &mut Frame, app: &App, area: Rect) {
    let Some(topology) = &app.topology else {
        render_unavailable(f, area);
        return;
    };

    let layers = TopologyLayers::new(topology);
    let lines = topology_lines(topology, &layers);
    let paragraph = Paragraph::new(lines).block(topology_block());
    f.render_widget(paragraph, area);
}

struct TopologyLayers<'a> {
    sources: Vec<&'a PipelineNode>,
    streams: Vec<&'a PipelineNode>,
    sinks: Vec<&'a PipelineNode>,
}

impl<'a> TopologyLayers<'a> {
    fn new(topology: &'a PipelineTopology) -> Self {
        Self {
            sources: nodes_of_type(topology, PipelineNodeType::Source),
            streams: nodes_of_type(topology, PipelineNodeType::Stream),
            sinks: nodes_of_type(topology, PipelineNodeType::Sink),
        }
    }
}

fn nodes_of_type(topology: &PipelineTopology, node_type: PipelineNodeType) -> Vec<&PipelineNode> {
    topology
        .nodes
        .iter()
        .filter(|node| node.node_type == node_type)
        .collect()
}

fn topology_lines<'a>(
    topology: &'a PipelineTopology,
    layers: &TopologyLayers<'a>,
) -> Vec<Line<'a>> {
    let mut lines = vec![Line::from("")];

    push_node_layer(&mut lines, &layers.sources, PipelineNodeType::Source);
    if !layers.sources.is_empty() && !layers.streams.is_empty() {
        push_outgoing_markers(&mut lines, &layers.sources, topology);
    }

    push_node_layer(&mut lines, &layers.streams, PipelineNodeType::Stream);
    push_stream_sql(&mut lines, &layers.streams);
    if !layers.streams.is_empty() && !layers.sinks.is_empty() {
        push_outgoing_markers(&mut lines, &layers.streams, topology);
    }

    push_node_layer(&mut lines, &layers.sinks, PipelineNodeType::Sink);
    push_edge_summary(&mut lines, topology);
    lines
}

fn push_node_layer<'a>(
    lines: &mut Vec<Line<'a>>,
    nodes: &[&'a PipelineNode],
    node_type: PipelineNodeType,
) {
    if nodes.is_empty() {
        return;
    }

    let mut spans = vec![Span::raw("  ")];
    for (index, node) in nodes.iter().enumerate() {
        if index > 0 {
            spans.push(Span::raw("    "));
        }
        spans.push(Span::styled(
            format!("[{}]", node.name),
            Style::default()
                .fg(node_color(node_type))
                .add_modifier(Modifier::BOLD),
        ));
        spans.push(Span::styled(
            node_label(node, node_type),
            Style::default().fg(Color::DarkGray),
        ));
    }
    lines.push(Line::from(spans));
}

fn node_color(node_type: PipelineNodeType) -> Color {
    match node_type {
        PipelineNodeType::Source => Color::Cyan,
        PipelineNodeType::Stream => Color::Green,
        PipelineNodeType::Sink => Color::Yellow,
    }
}

fn node_label(node: &PipelineNode, node_type: PipelineNodeType) -> String {
    match node_type {
        PipelineNodeType::Source => {
            let column_count = node
                .schema
                .as_ref()
                .map_or(0, |schema| schema.fields().len());
            format!(" SOURCE ({column_count} cols)")
        }
        PipelineNodeType::Stream => " STREAM".to_string(),
        PipelineNodeType::Sink => " SINK".to_string(),
    }
}

fn push_outgoing_markers<'a>(
    lines: &mut Vec<Line<'a>>,
    nodes: &[&PipelineNode],
    topology: &PipelineTopology,
) {
    let mut spans = vec![Span::raw("  ")];
    for (index, node) in nodes.iter().enumerate() {
        if index > 0 {
            spans.push(Span::raw("    "));
        }
        let marker = if topology.edges.iter().any(|edge| edge.from == node.name) {
            "|"
        } else {
            " "
        };
        spans.push(Span::styled(
            format!("  {:1$}", marker, node.name.len()),
            Style::default().fg(Color::DarkGray),
        ));
    }
    lines.push(Line::from(spans));
}

fn push_stream_sql<'a>(lines: &mut Vec<Line<'a>>, streams: &[&PipelineNode]) {
    for node in streams {
        if let Some(sql) = &node.sql {
            lines.push(Line::from(Span::styled(
                format_stream_sql(sql),
                Style::default().fg(Color::DarkGray),
            )));
        }
    }
}

fn format_stream_sql(sql: &str) -> String {
    let mut chars = sql.chars();
    let prefix: String = chars.by_ref().take(67).collect();
    if chars.next().is_some() {
        format!("    {prefix} ...")
    } else {
        format!("    {sql}")
    }
}

fn push_edge_summary<'a>(lines: &mut Vec<Line<'a>>, topology: &'a PipelineTopology) {
    lines.push(Line::from(""));
    lines.push(Line::from(Span::styled(
        format!(
            "  {} nodes, {} edges",
            topology.nodes.len(),
            topology.edges.len()
        ),
        Style::default().fg(Color::DarkGray),
    )));
    for edge in &topology.edges {
        lines.push(Line::from(vec![
            Span::raw("    "),
            Span::styled(&edge.from, Style::default().fg(Color::White)),
            Span::styled(" -> ", Style::default().fg(Color::DarkGray)),
            Span::styled(&edge.to, Style::default().fg(Color::White)),
        ]));
    }
}

fn render_unavailable(f: &mut Frame, area: Rect) {
    let message =
        Paragraph::new(" No topology available. Start the pipeline first.").block(topology_block());
    f.render_widget(message, area);
}

fn topology_block() -> Block<'static> {
    Block::default()
        .title(" PIPELINE TOPOLOGY ")
        .borders(Borders::ALL)
        .border_style(Style::default().fg(Color::Blue))
}

#[cfg(test)]
mod tests {
    use super::format_stream_sql;

    #[test]
    fn stream_sql_truncation_preserves_ascii_cutoff_and_handles_utf8() {
        let ascii = "x".repeat(71);
        assert_eq!(
            format_stream_sql(&ascii),
            format!("    {} ...", "x".repeat(67))
        );

        let unicode = "λ".repeat(71);
        assert_eq!(
            format_stream_sql(&unicode),
            format!("    {} ...", "λ".repeat(67))
        );
    }
}
