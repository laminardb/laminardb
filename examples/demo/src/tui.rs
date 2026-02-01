//! Ratatui TUI layout and widget rendering for the market data dashboard.

use ratatui::layout::{Constraint, Direction, Layout, Rect};
use ratatui::style::{Color, Modifier, Style};
use ratatui::text::{Line, Span};
use ratatui::widgets::{Block, Borders, Cell, Paragraph, Row, Sparkline, Table};
use ratatui::Frame;

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
    draw_main_content(f, app, chunks[2]);
    draw_footer(f, chunks[3]);
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

/// Footer: keyboard shortcuts.
fn draw_footer(f: &mut Frame, area: Rect) {
    let footer = Paragraph::new(Line::from(vec![
        Span::styled(" [q] ", Style::default().fg(Color::Yellow)),
        Span::raw("Quit  "),
        Span::styled("[Tab] ", Style::default().fg(Color::Yellow)),
        Span::raw("Symbol  "),
        Span::styled("[Space] ", Style::default().fg(Color::Yellow)),
        Span::raw("Pause  "),
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
