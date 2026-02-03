//! Ratatui TUI layout and widget rendering for the market data dashboard.

use ratatui::layout::{Constraint, Direction, Layout, Rect};
use ratatui::style::{Color, Modifier, Style};
use ratatui::text::{Line, Span};
use ratatui::widgets::{Block, Borders, Cell, Gauge, Paragraph, Row, Sparkline, Table};
use ratatui::Frame;

use laminar_db::PipelineNodeType;

use crate::app::App;
use crate::types::ViewMode;

/// Draw the entire dashboard.
pub fn draw(f: &mut Frame, app: &App) {
    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(3), // Header
            Constraint::Length(3), // KPI bar
            Constraint::Min(8),    // Main content
            Constraint::Length(3), // Footer
        ])
        .split(f.area());

    draw_header(f, app, chunks[0]);
    draw_kpi_bar(f, app, chunks[1]);

    match app.view_mode {
        ViewMode::Dashboard => draw_main_content(f, app, chunks[2]),
        ViewMode::OrderBook => draw_order_book_view(f, app, chunks[2]),
        ViewMode::Dag => draw_dag(f, app, chunks[2]),
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
        Span::styled(
            format!(" | Book: {}", format_count(app.total_book_updates)),
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

/// KPI bar: total volume, VWAP, spread, trades, ticks, CPU, memory.
fn draw_kpi_bar(f: &mut Frame, app: &App, area: Rect) {
    let cpu_color = if app.system_stats.cpu_usage > 80.0 {
        Color::Red
    } else if app.system_stats.cpu_usage > 50.0 {
        Color::Yellow
    } else {
        Color::Green
    };

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
        Span::styled(" | CPU: ", Style::default().fg(Color::DarkGray)),
        Span::styled(
            format!("{:.0}%", app.system_stats.cpu_usage),
            Style::default().fg(cpu_color),
        ),
        Span::styled(" | Mem: ", Style::default().fg(Color::DarkGray)),
        Span::styled(
            format!("{:.0}MB", app.system_stats.memory_mb),
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

    // Bottom row: Sparkline | Enriched Orders | Alerts
    let bot_cols = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([
            Constraint::Percentage(33),
            Constraint::Percentage(40),
            Constraint::Percentage(27),
        ])
        .split(rows[1]);

    draw_sparkline(f, app, bot_cols[0]);
    draw_enriched_orders(f, app, bot_cols[1]);
    draw_alerts(f, app, bot_cols[2]);
}

/// OHLC bars table.
fn draw_ohlc_table(f: &mut Frame, app: &App, area: Rect) {
    let header = Row::new(vec!["Symbol", "Low", "High", "VWAP", "Volume", "Trades"]).style(
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

/// Enriched orders table (ASOF JOIN results).
fn draw_enriched_orders(f: &mut Frame, app: &App, area: Rect) {
    let header = Row::new(vec!["Order", "Sym", "Side", "Qty", "Price", "Mkt", "Slip"]).style(
        Style::default()
            .fg(Color::Yellow)
            .add_modifier(Modifier::BOLD),
    );

    let max_rows = area.height.saturating_sub(3) as usize;
    let rows: Vec<Row> = app
        .enriched_orders
        .iter()
        .take(max_rows)
        .map(|eo| {
            let slip_color = if eo.slippage >= 0.0 {
                Color::Green
            } else {
                Color::Red
            };
            let slip_str = if eo.slippage >= 0.0 {
                format!("+{:.2}", eo.slippage)
            } else {
                format!("{:.2}", eo.slippage)
            };
            Row::new(vec![
                Cell::from(eo.order_id.chars().skip(4).collect::<String>())
                    .style(Style::default().fg(Color::DarkGray)),
                Cell::from(eo.symbol.clone()).style(Style::default().fg(Color::Cyan)),
                Cell::from(eo.side.clone()),
                Cell::from(eo.quantity.to_string()),
                Cell::from(format!("{:.2}", eo.order_price)),
                Cell::from(format!("{:.2}", eo.market_price)),
                Cell::from(slip_str).style(Style::default().fg(slip_color)),
            ])
        })
        .collect();

    let table = Table::new(
        rows,
        [
            Constraint::Length(9),
            Constraint::Length(6),
            Constraint::Length(5),
            Constraint::Length(4),
            Constraint::Length(7),
            Constraint::Length(7),
            Constraint::Length(7),
        ],
    )
    .header(header)
    .block(
        Block::default()
            .title(" ENRICHED ORDERS (ASOF) ")
            .borders(Borders::ALL)
            .border_style(Style::default().fg(Color::Magenta)),
    );
    f.render_widget(table, area);
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

// -- Order Book View --

/// Order book view: depth ladder (left) + analytics panel (right).
fn draw_order_book_view(f: &mut Frame, app: &App, area: Rect) {
    let cols = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([Constraint::Percentage(55), Constraint::Percentage(45)])
        .split(area);

    draw_order_book_depth(f, app, cols[0]);
    draw_book_analytics(f, app, cols[1]);
}

/// Depth ladder: asks (red, reversed) + spread separator + bids (green).
fn draw_order_book_depth(f: &mut Frame, app: &App, area: Rect) {
    let sym = app.selected_symbol();
    let mut lines: Vec<Line<'_>> = Vec::new();

    if let Some(book) = app.order_books.get(sym) {
        let asks = book.top_asks(10);
        let bids = book.top_bids(10);

        // Find max quantity for bar scaling
        let max_qty = asks
            .iter()
            .chain(bids.iter())
            .map(|l| l.quantity)
            .max()
            .unwrap_or(1)
            .max(1);

        // Asks: display in reverse order (highest ask at top, best ask at bottom)
        for level in asks.iter().rev() {
            let bar = qty_bar(level.quantity, max_qty, 10);
            lines.push(Line::from(vec![
                Span::styled("  ASK ", Style::default().fg(Color::Red)),
                Span::styled(
                    format!("{:>9.2}", level.price),
                    Style::default().fg(Color::Red).add_modifier(Modifier::BOLD),
                ),
                Span::styled(
                    format!("  {:>5}", level.quantity),
                    Style::default().fg(Color::White),
                ),
                Span::styled(
                    format!("  ({:>2})", level.order_count),
                    Style::default().fg(Color::DarkGray),
                ),
                Span::raw(" "),
                Span::styled(bar, Style::default().fg(Color::Red)),
            ]));
        }

        // Spread separator
        if let (Some(bb), Some(ba)) = (book.best_bid(), book.best_ask()) {
            let spread = ba.price - bb.price;
            let sep = format!(
                "  {:-^width$} SPREAD ${:.2} {:-^width$}",
                "",
                spread,
                "",
                width = 8
            );
            lines.push(Line::from(Span::styled(
                sep,
                Style::default().fg(Color::DarkGray),
            )));
        } else {
            lines.push(Line::from(Span::styled(
                "  --- NO SPREAD ---",
                Style::default().fg(Color::DarkGray),
            )));
        }

        // Bids: best bid at top (highest price first)
        for level in &bids {
            let bar = qty_bar(level.quantity, max_qty, 10);
            lines.push(Line::from(vec![
                Span::styled("  BID ", Style::default().fg(Color::Green)),
                Span::styled(
                    format!("{:>9.2}", level.price),
                    Style::default()
                        .fg(Color::Green)
                        .add_modifier(Modifier::BOLD),
                ),
                Span::styled(
                    format!("  {:>5}", level.quantity),
                    Style::default().fg(Color::White),
                ),
                Span::styled(
                    format!("  ({:>2})", level.order_count),
                    Style::default().fg(Color::DarkGray),
                ),
                Span::raw(" "),
                Span::styled(bar, Style::default().fg(Color::Green)),
            ]));
        }
    } else {
        lines.push(Line::from(Span::styled(
            "  Waiting for book updates...",
            Style::default().fg(Color::DarkGray),
        )));
    }

    let paragraph = Paragraph::new(lines).block(
        Block::default()
            .title(format!(" ORDER BOOK ({}) ", sym))
            .borders(Borders::ALL)
            .border_style(Style::default().fg(Color::Blue)),
    );
    f.render_widget(paragraph, area);
}

/// Analytics panel: imbalance, microprice, depth, spread, CPU, memory.
fn draw_book_analytics(f: &mut Frame, app: &App, area: Rect) {
    let rows = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(10), // Book metrics
            Constraint::Length(5),  // System stats
            Constraint::Min(4),     // Imbalance sparkline
        ])
        .split(area);

    draw_book_metrics(f, app, rows[0]);
    draw_system_stats(f, app, rows[1]);
    draw_imbalance_sparkline(f, app, rows[2]);
}

/// Book metrics: imbalance, microprice, depth, spread.
fn draw_book_metrics(f: &mut Frame, app: &App, area: Rect) {
    let sym = app.selected_symbol();
    let mut lines: Vec<Line<'_>> = Vec::new();

    // Imbalance from in-memory book state
    if let Some(book) = app.order_books.get(sym) {
        let imb = book.imbalance();
        let imb_pct = (imb * 100.0) as u16;
        let imb_color = if imb > 0.6 {
            Color::Green
        } else if imb < 0.4 {
            Color::Red
        } else {
            Color::Yellow
        };

        lines.push(Line::from(vec![
            Span::styled("  Imbalance: ", Style::default().fg(Color::DarkGray)),
            Span::styled(
                format!("{}%", imb_pct),
                Style::default().fg(imb_color).add_modifier(Modifier::BOLD),
            ),
            Span::styled(
                if imb > 0.6 {
                    " (bid heavy)"
                } else if imb < 0.4 {
                    " (ask heavy)"
                } else {
                    " (balanced)"
                },
                Style::default().fg(Color::DarkGray),
            ),
        ]));

        // Microprice
        if let Some(&mp) = app.microprices.get(sym) {
            lines.push(Line::from(vec![
                Span::styled("  Microprice: $", Style::default().fg(Color::DarkGray)),
                Span::styled(
                    format!("{:.3}", mp),
                    Style::default()
                        .fg(Color::Cyan)
                        .add_modifier(Modifier::BOLD),
                ),
            ]));
        }

        // Depth
        lines.push(Line::from(vec![
            Span::styled("  Bid Depth: ", Style::default().fg(Color::DarkGray)),
            Span::styled(
                format_count(book.bid_depth() as u64),
                Style::default().fg(Color::Green),
            ),
        ]));
        lines.push(Line::from(vec![
            Span::styled("  Ask Depth: ", Style::default().fg(Color::DarkGray)),
            Span::styled(
                format_count(book.ask_depth() as u64),
                Style::default().fg(Color::Red),
            ),
        ]));

        // Spread
        if let (Some(bb), Some(ba)) = (book.best_bid(), book.best_ask()) {
            let spread = ba.price - bb.price;
            let spread_bps = spread / bb.price * 10000.0;
            lines.push(Line::from(vec![
                Span::styled("  Spread: $", Style::default().fg(Color::DarkGray)),
                Span::styled(format!("{:.2}", spread), Style::default().fg(Color::Yellow)),
                Span::styled(
                    format!(" ({:.1} bps)", spread_bps),
                    Style::default().fg(Color::DarkGray),
                ),
            ]));
        }

        // Levels count
        lines.push(Line::from(vec![Span::styled(
            format!(
                "  Levels: {} bids / {} asks",
                book.bids.len(),
                book.asks.len()
            ),
            Style::default().fg(Color::DarkGray),
        )]));
    } else {
        lines.push(Line::from(Span::styled(
            "  No book data",
            Style::default().fg(Color::DarkGray),
        )));
    }

    let paragraph = Paragraph::new(lines).block(
        Block::default()
            .title(" ANALYTICS ")
            .borders(Borders::ALL)
            .border_style(Style::default().fg(Color::Magenta)),
    );
    f.render_widget(paragraph, area);
}

/// System stats: CPU gauge + memory.
fn draw_system_stats(f: &mut Frame, app: &App, area: Rect) {
    let inner = Layout::default()
        .direction(Direction::Vertical)
        .constraints([Constraint::Length(3), Constraint::Length(1)])
        .split(area);

    // CPU gauge
    let cpu_pct = app.system_stats.cpu_usage.min(100.0) as u16;
    let cpu_color = if cpu_pct > 80 {
        Color::Red
    } else if cpu_pct > 50 {
        Color::Yellow
    } else {
        Color::Green
    };

    let gauge = Gauge::default()
        .block(
            Block::default()
                .title(format!(
                    " CPU: {:.1}% | Mem: {:.0}/{:.0} MB ",
                    app.system_stats.cpu_usage,
                    app.system_stats.memory_mb,
                    app.system_stats.total_memory_mb,
                ))
                .borders(Borders::ALL)
                .border_style(Style::default().fg(Color::Blue)),
        )
        .gauge_style(Style::default().fg(cpu_color))
        .ratio((app.system_stats.cpu_usage as f64 / 100.0).clamp(0.0, 1.0));
    f.render_widget(gauge, inner[0]);
}

/// Imbalance history sparkline.
fn draw_imbalance_sparkline(f: &mut Frame, app: &App, area: Rect) {
    let data = app.imbalance_sparkline_data();
    let sym = app.selected_symbol();

    let sparkline = Sparkline::default()
        .block(
            Block::default()
                .title(format!(" IMBALANCE ({}) ", sym))
                .borders(Borders::ALL)
                .border_style(Style::default().fg(Color::Magenta)),
        )
        .data(&data)
        .max(100)
        .style(Style::default().fg(Color::Yellow));
    f.render_widget(sparkline, area);
}

// -- DAG View --

/// DAG pipeline view: renders the topology as a layered graph.
fn draw_dag(f: &mut Frame, app: &App, area: Rect) {
    let topo = match &app.topology {
        Some(t) => t,
        None => {
            let msg = Paragraph::new(" No topology available. Start the pipeline first.").block(
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
            let col_count = node.schema.as_ref().map_or(0, |s| s.fields().len());
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
        let mut arrow_spans = vec![Span::raw("  ")];
        for (i, src) in sources.iter().enumerate() {
            if i > 0 {
                arrow_spans.push(Span::raw("    "));
            }
            let feeds_any = topo.edges.iter().any(|e| e.from == src.name);
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
            let feeds_sink = topo.edges.iter().any(|e| e.from == stream.name);
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
            spans.push(Span::styled(" SINK", Style::default().fg(Color::DarkGray)));
        }
        lines.push(Line::from(spans));
    }

    // --- Edge summary ---
    lines.push(Line::from(""));
    lines.push(Line::from(Span::styled(
        format!("  {} nodes, {} edges", topo.nodes.len(), topo.edges.len()),
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
    let view_hint = match app.view_mode {
        ViewMode::Dashboard => "",
        ViewMode::OrderBook => " (Book)",
        ViewMode::Dag => " (Pipeline)",
    };
    let footer = Paragraph::new(Line::from(vec![
        Span::styled(" [q] ", Style::default().fg(Color::Yellow)),
        Span::raw("Quit  "),
        Span::styled("[Tab] ", Style::default().fg(Color::Yellow)),
        Span::raw("Symbol  "),
        Span::styled("[Space] ", Style::default().fg(Color::Yellow)),
        Span::raw("Pause  "),
        Span::styled("[b] ", Style::default().fg(Color::Yellow)),
        Span::raw("Book  "),
        Span::styled("[d] ", Style::default().fg(Color::Yellow)),
        Span::raw("Pipeline"),
        Span::styled(view_hint, Style::default().fg(Color::DarkGray)),
    ]))
    .block(Block::default().borders(Borders::ALL));
    f.render_widget(footer, area);
}

// -- Helpers --

/// Build a bar string proportional to quantity vs max.
fn qty_bar(qty: i64, max_qty: i64, max_width: usize) -> String {
    let ratio = qty as f64 / max_qty as f64;
    let full_blocks = (ratio * max_width as f64) as usize;
    let remainder = (ratio * max_width as f64) - full_blocks as f64;

    let mut bar = "\u{2588}".repeat(full_blocks); // full block
    if remainder > 0.5 {
        bar.push('\u{258C}'); // left half block
    }
    bar
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
