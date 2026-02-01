//! Ratatui TUI layout and widget rendering for the market data dashboard.

use ratatui::layout::{Constraint, Direction, Layout, Rect};
use ratatui::style::{Color, Modifier, Style};
use ratatui::text::{Line, Span};
use ratatui::widgets::{Block, Borders, Cell, Paragraph, Row, Sparkline, Table};
use ratatui::Frame;

use laminar_db::PipelineNodeType;

use crate::app::App;

/// Draw the entire dashboard.
pub fn draw(f: &mut Frame, app: &App) {
    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(3), // Header
            Constraint::Length(3), // KPI bar
            Constraint::Min(8),   // Main content
            Constraint::Length(3), // Footer
        ])
        .split(f.area());

    draw_header(f, app, chunks[0]);
    draw_kpi_bar(f, app, chunks[1]);
    if app.show_dag {
        draw_dag(f, app, chunks[2]);
    } else {
        draw_main_content(f, app, chunks[2]);
    }
    draw_footer(f, app, chunks[3]);
}

/// Header: title, status, uptime, throughput.
fn draw_header(f: &mut Frame, app: &App, area: Rect) {
    let uptime = app.uptime();
    let mins = uptime.as_secs() / 60;
    let secs = uptime.as_secs() % 60;
    let status = if app.paused { "Paused" } else { "Running" };
    let status_color = if app.paused {
        Color::Yellow
    } else {
        Color::Green
    };

    let header = Paragraph::new(Line::from(vec![
        Span::styled(
            " LAMINARDB MARKET DATA DEMO ",
            Style::default()
                .fg(Color::Cyan)
                .add_modifier(Modifier::BOLD),
        ),
        Span::raw(" "),
        Span::styled(status, Style::default().fg(status_color)),
        Span::styled(
            format!(" | {:02}:{:02}", mins, secs),
            Style::default().fg(Color::DarkGray),
        ),
        Span::styled(
            format!(" | {}/s", format_count(app.throughput())),
            Style::default().fg(Color::DarkGray),
        ),
    ]))
    .block(
        Block::default()
            .borders(Borders::ALL)
            .border_style(Style::default().fg(Color::Cyan)),
    );
    f.render_widget(header, area);
}

/// KPI bar: total volume, VWAP, spread, trades.
fn draw_kpi_bar(f: &mut Frame, app: &App, area: Rect) {
    let kpi = Paragraph::new(Line::from(vec![
        Span::styled(" Vol: ", Style::default().fg(Color::DarkGray)),
        Span::styled(
            format_count(app.total_volume() as u64),
            Style::default()
                .fg(Color::White)
                .add_modifier(Modifier::BOLD),
        ),
        Span::styled(" | VWAP: $", Style::default().fg(Color::DarkGray)),
        Span::styled(
            format!("{:.2}", app.avg_vwap()),
            Style::default()
                .fg(Color::Green)
                .add_modifier(Modifier::BOLD),
        ),
        Span::styled(" | Spread: ", Style::default().fg(Color::DarkGray)),
        Span::styled(
            format!("{:.4}", app.avg_spread()),
            Style::default().fg(Color::Yellow),
        ),
        Span::styled(" | Trades: ", Style::default().fg(Color::DarkGray)),
        Span::styled(
            format_count(app.total_trades() as u64),
            Style::default().fg(Color::White),
        ),
        Span::styled(" | Ticks: ", Style::default().fg(Color::DarkGray)),
        Span::styled(
            format_count(app.total_ticks),
            Style::default().fg(Color::DarkGray),
        ),
    ]))
    .block(Block::default().borders(Borders::ALL));
    f.render_widget(kpi, area);
}

/// Main content: OHLC + Order Flow on top, Sparkline + Alerts on bottom.
fn draw_main_content(f: &mut Frame, app: &App, area: Rect) {
    let rows = Layout::default()
        .direction(Direction::Vertical)
        .constraints([Constraint::Percentage(55), Constraint::Percentage(45)])
        .split(area);

    // Top row: OHLC | Order Flow
    let top_cols = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([Constraint::Percentage(50), Constraint::Percentage(50)])
        .split(rows[0]);

    draw_ohlc_table(f, app, top_cols[0]);
    draw_order_flow(f, app, top_cols[1]);

    // Bottom row: Sparkline | Alerts
    let bot_cols = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([Constraint::Percentage(50), Constraint::Percentage(50)])
        .split(rows[1]);

    draw_sparkline(f, app, bot_cols[0]);
    draw_alerts(f, app, bot_cols[1]);
}

/// OHLC bars table.
fn draw_ohlc_table(f: &mut Frame, app: &App, area: Rect) {
    let header = Row::new(vec!["Symbol", "Low", "High", "VWAP", "Volume", "Trades"])
        .style(
            Style::default()
                .fg(Color::Yellow)
                .add_modifier(Modifier::BOLD),
        );

    let symbols = app.symbols_ordered();
    let rows: Vec<Row> = symbols
        .iter()
        .map(|sym| {
            if let Some(bar) = app.ohlc.get(*sym) {
                let color = if bar.vwap >= bar.min_price {
                    Color::Green
                } else {
                    Color::Red
                };
                Row::new(vec![
                    Cell::from(sym.to_string()).style(Style::default().fg(Color::Cyan)),
                    Cell::from(format!("{:.2}", bar.min_price)),
                    Cell::from(format!("{:.2}", bar.max_price)),
                    Cell::from(format!("{:.2}", bar.vwap)).style(Style::default().fg(color)),
                    Cell::from(format_count(bar.total_volume as u64)),
                    Cell::from(format_count(bar.trade_count as u64)),
                ])
            } else {
                Row::new(vec![
                    Cell::from(sym.to_string()).style(Style::default().fg(Color::DarkGray)),
                    Cell::from("--"),
                    Cell::from("--"),
                    Cell::from("--"),
                    Cell::from("--"),
                    Cell::from("--"),
                ])
            }
        })
        .collect();

    let table = Table::new(
        rows,
        [
            Constraint::Length(7),
            Constraint::Length(9),
            Constraint::Length(9),
            Constraint::Length(9),
            Constraint::Length(8),
            Constraint::Length(8),
        ],
    )
    .header(header)
    .block(
        Block::default()
            .title(" OHLC BARS ")
            .borders(Borders::ALL)
            .border_style(Style::default().fg(Color::Blue)),
    );
    f.render_widget(table, area);
}

/// Order flow table (buy/sell volume).
fn draw_order_flow(f: &mut Frame, app: &App, area: Rect) {
    let header = Row::new(vec!["Symbol", "Buys", "Sells", "Net", "Trades"]).style(
        Style::default()
            .fg(Color::Yellow)
            .add_modifier(Modifier::BOLD),
    );

    let symbols = app.symbols_ordered();
    let rows: Vec<Row> = symbols
        .iter()
        .map(|sym| {
            if let Some(vol) = app.volume.get(*sym) {
                let net_color = if vol.net_volume >= 0 {
                    Color::Green
                } else {
                    Color::Red
                };
                let net_str = if vol.net_volume >= 0 {
                    format!("+{}", format_count(vol.net_volume as u64))
                } else {
                    format!("-{}", format_count((-vol.net_volume) as u64))
                };
                Row::new(vec![
                    Cell::from(sym.to_string()).style(Style::default().fg(Color::Cyan)),
                    Cell::from(format_count(vol.buy_volume as u64))
                        .style(Style::default().fg(Color::Green)),
                    Cell::from(format_count(vol.sell_volume as u64))
                        .style(Style::default().fg(Color::Red)),
                    Cell::from(net_str).style(Style::default().fg(net_color)),
                    Cell::from(format_count(vol.trade_count as u64)),
                ])
            } else {
                Row::new(vec![
                    Cell::from(sym.to_string()).style(Style::default().fg(Color::DarkGray)),
                    Cell::from("--"),
                    Cell::from("--"),
                    Cell::from("--"),
                    Cell::from("--"),
                ])
            }
        })
        .collect();

    let table = Table::new(
        rows,
        [
            Constraint::Length(7),
            Constraint::Length(9),
            Constraint::Length(9),
            Constraint::Length(9),
            Constraint::Length(8),
        ],
    )
    .header(header)
    .block(
        Block::default()
            .title(" ORDER FLOW ")
            .borders(Borders::ALL)
            .border_style(Style::default().fg(Color::Blue)),
    );
    f.render_widget(table, area);
}

/// Price sparkline for the selected symbol.
fn draw_sparkline(f: &mut Frame, app: &App, area: Rect) {
    let data = app.sparkline_data();
    let sym = app.selected_symbol();

    let sparkline = Sparkline::default()
        .block(
            Block::default()
                .title(format!(" PRICE ({}) ", sym))
                .borders(Borders::ALL)
                .border_style(Style::default().fg(Color::Blue)),
        )
        .data(&data)
        .style(Style::default().fg(Color::Green));
    f.render_widget(sparkline, area);
}

/// Alert list.
fn draw_alerts(f: &mut Frame, app: &App, area: Rect) {
    let max_lines = area.height.saturating_sub(2) as usize;
    let lines: Vec<Line> = app
        .alerts
        .iter()
        .take(max_lines)
        .map(|alert| {
            Line::from(vec![
                Span::styled(
                    format!("{} ", alert.time),
                    Style::default().fg(Color::DarkGray),
                ),
                Span::styled(&alert.message, Style::default().fg(Color::Yellow)),
            ])
        })
        .collect();

    let paragraph = Paragraph::new(lines).block(
        Block::default()
            .title(" ALERTS ")
            .borders(Borders::ALL)
            .border_style(Style::default().fg(Color::Red)),
    );
    f.render_widget(paragraph, area);
}

/// DAG pipeline view: renders the topology as a layered graph.
fn draw_dag(f: &mut Frame, app: &App, area: Rect) {
    let topo = match &app.topology {
        Some(t) => t,
        None => {
            let msg = Paragraph::new(" No topology available. Start the pipeline first.")
                .block(
                    Block::default()
                        .title(" PIPELINE TOPOLOGY ")
                        .borders(Borders::ALL)
                        .border_style(Style::default().fg(Color::Blue)),
                );
            f.render_widget(msg, area);
            return;
        }
    };

    let mut lines: Vec<Line<'_>> = Vec::new();
    lines.push(Line::from(""));

    // Partition nodes into layers
    let sources: Vec<_> = topo
        .nodes
        .iter()
        .filter(|n| n.node_type == PipelineNodeType::Source)
        .collect();
    let streams: Vec<_> = topo
        .nodes
        .iter()
        .filter(|n| n.node_type == PipelineNodeType::Stream)
        .collect();
    let sinks: Vec<_> = topo
        .nodes
        .iter()
        .filter(|n| n.node_type == PipelineNodeType::Sink)
        .collect();

    // --- Source layer ---
    if !sources.is_empty() {
        let mut spans = vec![Span::raw("  ")];
        for (i, node) in sources.iter().enumerate() {
            if i > 0 {
                spans.push(Span::raw("    "));
            }
            let col_count = node
                .schema
                .as_ref()
                .map_or(0, |s| s.fields().len());
            spans.push(Span::styled(
                format!("[{}]", node.name),
                Style::default()
                    .fg(Color::Cyan)
                    .add_modifier(Modifier::BOLD),
            ));
            spans.push(Span::styled(
                format!(" SOURCE ({col_count} cols)"),
                Style::default().fg(Color::DarkGray),
            ));
        }
        lines.push(Line::from(spans));
    }

    // --- Arrows from sources to streams ---
    if !sources.is_empty() && !streams.is_empty() {
        // Build a connection indicator line
        let mut arrow_spans = vec![Span::raw("  ")];
        for (i, src) in sources.iter().enumerate() {
            if i > 0 {
                arrow_spans.push(Span::raw("    "));
            }
            // Count how many streams this source feeds
            let feeds_any = topo
                .edges
                .iter()
                .any(|e| e.from == src.name);
            let indicator = if feeds_any {
                format!("  {:1$}", "|", src.name.len())
            } else {
                format!("  {:1$}", " ", src.name.len())
            };
            arrow_spans.push(Span::styled(
                indicator,
                Style::default().fg(Color::DarkGray),
            ));
        }
        lines.push(Line::from(arrow_spans));
    }

    // --- Stream layer ---
    if !streams.is_empty() {
        let mut spans = vec![Span::raw("  ")];
        for (i, node) in streams.iter().enumerate() {
            if i > 0 {
                spans.push(Span::raw("    "));
            }
            spans.push(Span::styled(
                format!("[{}]", node.name),
                Style::default()
                    .fg(Color::Green)
                    .add_modifier(Modifier::BOLD),
            ));
            spans.push(Span::styled(
                " STREAM",
                Style::default().fg(Color::DarkGray),
            ));
        }
        lines.push(Line::from(spans));

        // Show SQL for each stream
        for node in &streams {
            if let Some(sql) = &node.sql {
                let truncated = if sql.len() > 70 {
                    format!("    {} ...", &sql[..67])
                } else {
                    format!("    {sql}")
                };
                lines.push(Line::from(Span::styled(
                    truncated,
                    Style::default().fg(Color::DarkGray),
                )));
            }
        }
    }

    // --- Arrows from streams to sinks ---
    if !streams.is_empty() && !sinks.is_empty() {
        let mut arrow_spans = vec![Span::raw("  ")];
        for (i, stream) in streams.iter().enumerate() {
            if i > 0 {
                arrow_spans.push(Span::raw("    "));
            }
            let feeds_sink = topo
                .edges
                .iter()
                .any(|e| e.from == stream.name);
            let indicator = if feeds_sink {
                format!("  {:1$}", "|", stream.name.len())
            } else {
                format!("  {:1$}", " ", stream.name.len())
            };
            arrow_spans.push(Span::styled(
                indicator,
                Style::default().fg(Color::DarkGray),
            ));
        }
        lines.push(Line::from(arrow_spans));
    }

    // --- Sink layer ---
    if !sinks.is_empty() {
        let mut spans = vec![Span::raw("  ")];
        for (i, node) in sinks.iter().enumerate() {
            if i > 0 {
                spans.push(Span::raw("    "));
            }
            spans.push(Span::styled(
                format!("[{}]", node.name),
                Style::default()
                    .fg(Color::Yellow)
                    .add_modifier(Modifier::BOLD),
            ));
            spans.push(Span::styled(
                " SINK",
                Style::default().fg(Color::DarkGray),
            ));
        }
        lines.push(Line::from(spans));
    }

    // --- Edge summary ---
    lines.push(Line::from(""));
    lines.push(Line::from(Span::styled(
        format!(
            "  {} nodes, {} edges",
            topo.nodes.len(),
            topo.edges.len()
        ),
        Style::default().fg(Color::DarkGray),
    )));

    // List edges
    for edge in &topo.edges {
        lines.push(Line::from(vec![
            Span::raw("    "),
            Span::styled(&edge.from, Style::default().fg(Color::White)),
            Span::styled(" -> ", Style::default().fg(Color::DarkGray)),
            Span::styled(&edge.to, Style::default().fg(Color::White)),
        ]));
    }

    let paragraph = Paragraph::new(lines).block(
        Block::default()
            .title(" PIPELINE TOPOLOGY ")
            .borders(Borders::ALL)
            .border_style(Style::default().fg(Color::Blue)),
    );
    f.render_widget(paragraph, area);
}

/// Footer: keyboard shortcuts.
fn draw_footer(f: &mut Frame, app: &App, area: Rect) {
    let dag_label = if app.show_dag { "Dashboard" } else { "Pipeline" };
    let footer = Paragraph::new(Line::from(vec![
        Span::styled(" [q] ", Style::default().fg(Color::Yellow)),
        Span::raw("Quit  "),
        Span::styled("[Tab] ", Style::default().fg(Color::Yellow)),
        Span::raw("Symbol  "),
        Span::styled("[Space] ", Style::default().fg(Color::Yellow)),
        Span::raw("Pause  "),
        Span::styled("[d] ", Style::default().fg(Color::Yellow)),
        Span::raw(dag_label),
    ]))
    .block(Block::default().borders(Borders::ALL));
    f.render_widget(footer, area);
}

/// Format a count with K/M suffixes.
fn format_count(n: u64) -> String {
    if n >= 1_000_000 {
        format!("{:.1}M", n as f64 / 1_000_000.0)
    } else if n >= 1_000 {
        format!("{:.1}K", n as f64 / 1_000.0)
    } else {
        n.to_string()
    }
}
