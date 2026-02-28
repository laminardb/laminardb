#![allow(clippy::disallowed_types)]

use std::collections::HashMap;
use std::time::{Duration, Instant};

use ratatui::layout::{Constraint, Layout, Rect};
use ratatui::style::{Color, Modifier, Style};
use ratatui::text::{Line, Span};
use ratatui::widgets::{Block, Borders, Cell, Padding, Paragraph, Row, Table};

use crate::types::{Signal, Vwap, LABELS, SYMBOLS};

pub struct DashState {
    pub vwap: HashMap<String, Vwap>,
    pub signals: HashMap<String, Signal>,
    pub start: Instant,
    pub total_events: u64,
    pub throughput: f64,
    pub cycle_ns: u64,
    pub source_count: usize,
    pub stream_count: usize,
}

pub fn render(frame: &mut ratatui::Frame, s: &DashState) {
    let chunks = Layout::vertical([
        Constraint::Length(3), // banner
        Constraint::Min(9),    // vwap table
        Constraint::Min(6),    // signals table
        Constraint::Length(3), // footer
    ])
    .split(frame.area());

    draw_banner(frame, chunks[0], s);
    draw_vwap(frame, chunks[1], &s.vwap);
    draw_signals(frame, chunks[2], &s.signals);
    draw_footer(frame, chunks[3], s);
}

fn draw_banner(frame: &mut ratatui::Frame, area: Rect, s: &DashState) {
    let uptime = fmt_duration(s.start.elapsed());
    let banner = Paragraph::new(Line::from(vec![
        Span::styled(
            " LaminarDB",
            Style::default()
                .fg(Color::Green)
                .add_modifier(Modifier::BOLD),
        ),
        Span::styled("  |  ", Style::default().fg(Color::DarkGray)),
        Span::styled(
            format!("{} sources", s.source_count),
            Style::default().fg(Color::Cyan),
        ),
        Span::styled("  ", Style::default()),
        Span::styled(
            format!("{} views", s.stream_count),
            Style::default().fg(Color::Cyan),
        ),
        Span::styled("  ", Style::default()),
        Span::styled(
            format!("{}/s", fmt_int(s.throughput as u64)),
            Style::default().fg(Color::Cyan),
        ),
        Span::styled("  ", Style::default()),
        Span::styled(uptime, Style::default().fg(Color::DarkGray)),
    ]))
    .block(
        Block::default()
            .borders(Borders::BOTTOM)
            .border_style(Style::default().fg(Color::DarkGray)),
    );
    frame.render_widget(banner, area);
}

fn draw_vwap(frame: &mut ratatui::Frame, area: Rect, data: &HashMap<String, Vwap>) {
    let header = Row::new(vec![
        Cell::from("symbol"),
        Cell::from("vwap"),
        Cell::from("volume"),
        Cell::from("trades"),
        Cell::from("high"),
        Cell::from("low"),
        Cell::from("volatility"),
    ])
    .style(Style::default().fg(Color::DarkGray))
    .height(1);

    let mut rows = Vec::new();
    for (i, sym) in SYMBOLS.iter().enumerate() {
        let key = sym.to_uppercase();
        if let Some(v) = data.get(&key) {
            rows.push(Row::new(vec![
                Cell::from(format!(" {}", LABELS[i]))
                    .style(Style::default().add_modifier(Modifier::BOLD)),
                Cell::from(fmt_price(v.vwap)).style(Style::default().fg(Color::Green)),
                Cell::from(fmt_qty(v.volume)),
                Cell::from(fmt_int(v.trades as u64)).style(Style::default().fg(Color::Cyan)),
                Cell::from(fmt_price(v.high)).style(Style::default().fg(Color::Green)),
                Cell::from(fmt_price(v.low)).style(Style::default().fg(Color::Red)),
                Cell::from(format!("{:.1}bp", v.volatility_bps))
                    .style(Style::default().fg(Color::Yellow)),
            ]));
        }
    }
    if rows.is_empty() {
        rows.push(Row::new(vec![Cell::from(
            " waiting for first 10s window...",
        )
        .style(Style::default().fg(Color::DarkGray))]));
    }

    let table = Table::new(
        rows,
        [
            Constraint::Length(8),
            Constraint::Length(14),
            Constraint::Length(11),
            Constraint::Length(8),
            Constraint::Length(14),
            Constraint::Length(14),
            Constraint::Length(10),
        ],
    )
    .header(header)
    .block(
        Block::default()
            .title(Line::from(vec![Span::styled(
                " VWAP (10s) ",
                Style::default()
                    .fg(Color::Yellow)
                    .add_modifier(Modifier::BOLD),
            )]))
            .borders(Borders::ALL)
            .border_style(Style::default().fg(Color::DarkGray))
            .padding(Padding::horizontal(1)),
    );
    frame.render_widget(table, area);
}

fn draw_signals(frame: &mut ratatui::Frame, area: Rect, data: &HashMap<String, Signal>) {
    let header = Row::new(vec![
        Cell::from("symbol"),
        Cell::from("signal"),
        Cell::from("vwap"),
        Cell::from("avg"),
        Cell::from("volume"),
        Cell::from("volatility"),
    ])
    .style(Style::default().fg(Color::DarkGray))
    .height(1);

    let mut rows = Vec::new();
    for (i, sym) in SYMBOLS.iter().enumerate() {
        let key = sym.to_uppercase();
        if let Some(a) = data.get(&key) {
            let sig_style = match a.signal.as_str() {
                "BUY" => Style::default()
                    .fg(Color::Green)
                    .add_modifier(Modifier::BOLD),
                "SELL" => Style::default().fg(Color::Red).add_modifier(Modifier::BOLD),
                _ => Style::default().fg(Color::DarkGray),
            };
            rows.push(Row::new(vec![
                Cell::from(format!(" {}", LABELS[i]))
                    .style(Style::default().add_modifier(Modifier::BOLD)),
                Cell::from(format!("{:>6}", a.signal)).style(sig_style),
                Cell::from(fmt_price(a.vwap)).style(Style::default().fg(Color::Green)),
                Cell::from(fmt_price(a.avg_price)).style(Style::default().fg(Color::Green)),
                Cell::from(fmt_qty(a.volume)),
                Cell::from(format!("{:.1}bp", a.volatility_bps))
                    .style(Style::default().fg(Color::Yellow)),
            ]));
        }
    }
    if rows.is_empty() {
        rows.push(Row::new(vec![
            Cell::from(" waiting for first window...").style(Style::default().fg(Color::DarkGray))
        ]));
    }

    let table = Table::new(
        rows,
        [
            Constraint::Length(8),
            Constraint::Length(8),
            Constraint::Length(14),
            Constraint::Length(14),
            Constraint::Length(11),
            Constraint::Length(10),
        ],
    )
    .header(header)
    .block(
        Block::default()
            .title(Line::from(vec![Span::styled(
                " Signals ",
                Style::default().fg(Color::Red).add_modifier(Modifier::BOLD),
            )]))
            .borders(Borders::ALL)
            .border_style(Style::default().fg(Color::DarkGray))
            .padding(Padding::horizontal(1)),
    );
    frame.render_widget(table, area);
}

fn draw_footer(frame: &mut ratatui::Frame, area: Rect, s: &DashState) {
    let footer = Paragraph::new(Line::from(vec![
        Span::styled(" events: ", Style::default().fg(Color::DarkGray)),
        Span::styled(fmt_int(s.total_events), Style::default().fg(Color::White)),
        Span::styled("  |  ", Style::default().fg(Color::DarkGray)),
        Span::styled(
            format!("{}/s", fmt_int(s.throughput as u64)),
            Style::default().fg(Color::Cyan),
        ),
        Span::styled("  |  ", Style::default().fg(Color::DarkGray)),
        Span::styled("cycle: ", Style::default().fg(Color::DarkGray)),
        Span::styled(
            format!("{}ns", fmt_int(s.cycle_ns)),
            Style::default().fg(Color::Cyan),
        ),
        Span::styled(
            "                              q: quit",
            Style::default().fg(Color::DarkGray),
        ),
    ]))
    .block(
        Block::default()
            .borders(Borders::TOP)
            .border_style(Style::default().fg(Color::DarkGray)),
    );
    frame.render_widget(footer, area);
}

// ── Formatting helpers ───────────────────────────────────────────

fn fmt_price(v: f64) -> String {
    if v < 1.0 {
        return format!("${v:.4}");
    }
    let s = format!("{:.2}", v.abs());
    let mut parts = s.splitn(2, '.');
    let whole = parts.next().unwrap_or("0");
    let frac = parts.next().unwrap_or("00");
    let with_commas: String = whole
        .as_bytes()
        .rchunks(3)
        .rev()
        .map(|c| std::str::from_utf8(c).unwrap())
        .collect::<Vec<_>>()
        .join(",");
    if v < 0.0 {
        format!("-${with_commas}.{frac}")
    } else {
        format!("${with_commas}.{frac}")
    }
}

fn fmt_qty(v: f64) -> String {
    if v >= 1_000.0 {
        let s = format!("{v:.1}");
        let mut parts = s.splitn(2, '.');
        let whole = parts.next().unwrap_or("0");
        let frac = parts.next().unwrap_or("0");
        let with_commas: String = whole
            .as_bytes()
            .rchunks(3)
            .rev()
            .map(|c| std::str::from_utf8(c).unwrap())
            .collect::<Vec<_>>()
            .join(",");
        format!("{with_commas}.{frac}")
    } else {
        format!("{v:.4}")
    }
}

fn fmt_int(v: u64) -> String {
    let s = v.to_string();
    s.as_bytes()
        .rchunks(3)
        .rev()
        .map(|c| std::str::from_utf8(c).unwrap())
        .collect::<Vec<_>>()
        .join(",")
}

fn fmt_duration(d: Duration) -> String {
    let secs = d.as_secs();
    if secs < 60 {
        format!("{secs}s")
    } else if secs < 3600 {
        format!("{}m {}s", secs / 60, secs % 60)
    } else {
        format!("{}h {}m", secs / 3600, (secs % 3600) / 60)
    }
}
