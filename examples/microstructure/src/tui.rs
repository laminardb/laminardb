#![allow(clippy::disallowed_types)]

use std::collections::HashMap;
use std::time::{Duration, Instant};

use ratatui::layout::{Constraint, Layout, Rect};
use ratatui::style::{Color, Modifier, Style};
use ratatui::symbols::Marker;
use ratatui::text::{Line, Span};
use ratatui::widgets::{Axis, Block, Borders, Chart, Dataset, Padding, Paragraph, Sparkline};

use crate::history::RingBuf;
use crate::latency::LatencyTracker;
use crate::types::{
    Burst, DepthLevel, MicroCandle, Momentum, Signal, Spread, Vwap, LABELS, SYMBOLS,
};

// ── LaminarDB brand palette ──────────────────────────────────
const CYAN: Color = Color::Rgb(6, 182, 212);
const VIOLET: Color = Color::Rgb(139, 92, 246);
const AMBER: Color = Color::Rgb(251, 191, 36);
const GREEN: Color = Color::Rgb(74, 222, 128);
const ROSE: Color = Color::Rgb(251, 113, 133);
const TEXT_PRI: Color = Color::Rgb(241, 245, 249);
const TEXT_SEC: Color = Color::Rgb(148, 163, 184);
const TEXT_MUT: Color = Color::Rgb(100, 116, 139);
const BORDER: Color = Color::Rgb(51, 65, 85);

const SYMBOL_COLORS: &[Color] = &[
    AMBER,                     // BTC
    CYAN,                      // ETH
    VIOLET,                    // SOL
    GREEN,                     // DOGE
    Color::Rgb(103, 232, 249), // XRP — cyan-300
    ROSE,                      // BNB
];

pub struct DashState {
    pub spreads: HashMap<String, Spread>,
    pub vwap: HashMap<String, Vwap>,
    pub momentum: HashMap<String, Momentum>,
    pub bursts: HashMap<String, Burst>,
    pub signals: HashMap<String, Signal>,
    pub micro_candles: HashMap<String, MicroCandle>,
    pub price_history: HashMap<String, RingBuf<f64>>,
    pub depth_bids: Vec<DepthLevel>,
    pub depth_asks: Vec<DepthLevel>,
    pub latency_history: RingBuf<u64>,
    pub start: Instant,
    pub total_events: u64,
    pub total_emitted: u64,
    pub total_dropped: u64,
    pub throughput: f64,
    pub source_count: usize,
    pub stream_count: usize,
    pub pipeline_watermark: i64,
    pub latency: LatencyTracker,
}

impl DashState {
    pub fn new() -> Self {
        Self {
            spreads: HashMap::new(),
            vwap: HashMap::new(),
            momentum: HashMap::new(),
            bursts: HashMap::new(),
            signals: HashMap::new(),
            micro_candles: HashMap::new(),
            price_history: HashMap::new(),
            depth_bids: Vec::new(),
            depth_asks: Vec::new(),
            latency_history: RingBuf::new(120),
            start: Instant::now(),
            total_events: 0,
            total_emitted: 0,
            total_dropped: 0,
            throughput: 0.0,
            source_count: 0,
            stream_count: 0,
            pipeline_watermark: 0,
            latency: LatencyTracker::new(),
        }
    }
}

// ── Main render ────────────────────────────────────────────────

pub fn render(frame: &mut ratatui::Frame, s: &DashState) {
    let outer = Layout::vertical([
        Constraint::Length(2), // banner
        Constraint::Min(10),   // main area
        Constraint::Length(2), // footer
    ])
    .split(frame.area());

    draw_banner(frame, outer[0], s);

    let rows =
        Layout::vertical([Constraint::Percentage(55), Constraint::Percentage(45)]).split(outer[1]);

    let top = Layout::horizontal([
        Constraint::Length(24), // tickers
        Constraint::Min(20),    // price chart
        Constraint::Length(30), // depth orderbook
    ])
    .split(rows[0]);

    let bottom = Layout::horizontal([
        Constraint::Length(24), // latency
        Constraint::Min(16),    // momentum
        Constraint::Length(18), // signals
        Constraint::Length(20), // bursts
    ])
    .split(rows[1]);

    draw_tickers(frame, top[0], s);
    draw_price_chart(frame, top[1], s);
    draw_depth(frame, top[2], s);

    draw_latency(frame, bottom[0], s);
    draw_momentum(frame, bottom[1], s);
    draw_signals(frame, bottom[2], s);
    draw_bursts(frame, bottom[3], s);

    draw_footer(frame, outer[2], s);
}

// ── Banner ─────────────────────────────────────────────────────

fn draw_banner(frame: &mut ratatui::Frame, area: Rect, s: &DashState) {
    let uptime = fmt_duration(s.start.elapsed());
    let banner = Paragraph::new(Line::from(vec![
        Span::styled(
            " LaminarDB",
            Style::default().fg(CYAN).add_modifier(Modifier::BOLD),
        ),
        Span::styled(
            " Microstructure ",
            Style::default().fg(VIOLET).add_modifier(Modifier::BOLD),
        ),
        Span::styled("| ", Style::default().fg(TEXT_MUT)),
        Span::styled(format!("{} src", s.source_count), Style::default().fg(CYAN)),
        Span::styled("  ", Style::default()),
        Span::styled(
            format!("{} streams", s.stream_count),
            Style::default().fg(CYAN),
        ),
        Span::styled("  ", Style::default()),
        Span::styled(
            format!("{}/s", fmt_int(s.throughput as u64)),
            Style::default().fg(GREEN),
        ),
        Span::styled("  ", Style::default()),
        Span::styled(
            format!("p50 {}", fmt_latency(s.latency.p50())),
            Style::default().fg(CYAN),
        ),
        Span::styled("  ", Style::default()),
        Span::styled(uptime, Style::default().fg(TEXT_MUT)),
    ]))
    .block(
        Block::default()
            .borders(Borders::BOTTOM)
            .border_style(Style::default().fg(BORDER)),
    );
    frame.render_widget(banner, area);
}

// ── Tickers (left column, top) ─────────────────────────────────

fn draw_tickers(frame: &mut ratatui::Frame, area: Rect, s: &DashState) {
    let mut lines = Vec::new();
    for (i, sym) in SYMBOLS.iter().enumerate() {
        let key = sym.to_uppercase();

        let (price_str, arrow, arrow_color) = if let Some(v) = s.vwap.get(&key) {
            let (a, c) = if v.avg_price > v.vwap * 1.0001 {
                (" \u{25b2}", GREEN) // ▲
            } else if v.avg_price < v.vwap * 0.9999 {
                (" \u{25bc}", ROSE) // ▼
            } else {
                (" \u{2500}", TEXT_MUT) // ─
            };
            (fmt_price_compact(v.avg_price), a, c)
        } else {
            ("-".to_string(), " \u{2500}", TEXT_MUT)
        };

        let spread = s
            .spreads
            .get(&key)
            .map(|sp| format!("{:>4.1}", sp.spread_bps))
            .unwrap_or_else(|| "   -".to_string());

        lines.push(Line::from(vec![
            Span::styled(
                format!("{:>4}", LABELS[i]),
                Style::default()
                    .fg(SYMBOL_COLORS[i])
                    .add_modifier(Modifier::BOLD),
            ),
            Span::styled(arrow, Style::default().fg(arrow_color)),
            Span::styled(format!(" {:>9}", price_str), Style::default().fg(TEXT_PRI)),
            Span::styled(spread, Style::default().fg(AMBER)),
        ]));
    }

    let paragraph = Paragraph::new(lines).block(
        Block::default()
            .title(Line::from(Span::styled(
                " TICKERS ",
                Style::default().fg(AMBER).add_modifier(Modifier::BOLD),
            )))
            .borders(Borders::ALL)
            .border_style(Style::default().fg(BORDER))
            .padding(Padding::horizontal(1)),
    );
    frame.render_widget(paragraph, area);
}

// ── Price chart (center, top) ──────────────────────────────────

fn draw_price_chart(frame: &mut ratatui::Frame, area: Rect, s: &DashState) {
    let mut all_data: Vec<(usize, Vec<(f64, f64)>)> = Vec::new();
    let mut y_min = f64::MAX;
    let mut y_max = f64::MIN;
    let mut max_len: usize = 0;

    for (i, sym) in SYMBOLS.iter().enumerate() {
        let key = sym.to_uppercase();
        if let Some(hist) = s.price_history.get(&key) {
            if hist.len() >= 2 {
                let slice: Vec<f64> = hist.as_slice().iter().copied().collect();
                let base = slice[0];
                if base == 0.0 {
                    continue;
                }
                let points: Vec<(f64, f64)> = slice
                    .iter()
                    .enumerate()
                    .map(|(j, &v)| (j as f64, (v - base) / base * 100.0))
                    .collect();
                for &(_, v) in &points {
                    if v < y_min {
                        y_min = v;
                    }
                    if v > y_max {
                        y_max = v;
                    }
                }
                if points.len() > max_len {
                    max_len = points.len();
                }
                all_data.push((i, points));
            }
        }
    }

    if all_data.is_empty() || y_min >= y_max {
        y_min = -0.5;
        y_max = 0.5;
    }

    let range = y_max - y_min;
    let pad = (range * 0.1).max(0.01);
    y_min -= pad;
    y_max += pad;

    let x_max = if max_len > 1 {
        (max_len - 1) as f64
    } else {
        1.0
    };

    let mut title_parts = vec![Span::styled(
        " PRICE ",
        Style::default().fg(AMBER).add_modifier(Modifier::BOLD),
    )];
    for (i, _) in &all_data {
        title_parts.push(Span::styled(
            format!(" {} ", LABELS[*i]),
            Style::default().fg(SYMBOL_COLORS[*i]),
        ));
    }
    if all_data.is_empty() {
        title_parts.push(Span::styled(
            " waiting for data... ",
            Style::default().fg(TEXT_MUT),
        ));
    }

    let datasets: Vec<Dataset> = all_data
        .iter()
        .map(|(i, points)| {
            Dataset::default()
                .name(LABELS[*i])
                .marker(Marker::Braille)
                .style(Style::default().fg(SYMBOL_COLORS[*i]))
                .data(points)
        })
        .collect();

    let chart = Chart::new(datasets)
        .block(
            Block::default()
                .title(Line::from(title_parts))
                .borders(Borders::ALL)
                .border_style(Style::default().fg(BORDER)),
        )
        .x_axis(
            Axis::default()
                .style(Style::default().fg(TEXT_MUT))
                .bounds([0.0, x_max])
                .labels(vec![
                    Span::styled("-5m", Style::default().fg(TEXT_MUT)),
                    Span::styled("now", Style::default().fg(TEXT_MUT)),
                ]),
        )
        .y_axis(
            Axis::default()
                .style(Style::default().fg(TEXT_MUT))
                .bounds([y_min, y_max])
                .labels(vec![
                    Span::styled(format!("{:+.2}%", y_min), Style::default().fg(TEXT_MUT)),
                    Span::styled("  0%", Style::default().fg(TEXT_MUT)),
                    Span::styled(format!("{:+.2}%", y_max), Style::default().fg(TEXT_MUT)),
                ]),
        );
    frame.render_widget(chart, area);
}

// ── BTC Depth Orderbook (right column, top) ─────────────────────

fn draw_depth(frame: &mut ratatui::Frame, area: Rect, s: &DashState) {
    let max_levels = 5;
    let mut lines = Vec::new();

    if s.depth_asks.is_empty() && s.depth_bids.is_empty() {
        lines.push(Line::from(Span::styled(
            " waiting for depth...",
            Style::default().fg(TEXT_MUT),
        )));
    } else {
        // Asks: show lowest N asks, displayed top-to-bottom (highest first)
        let ask_count = s.depth_asks.len().min(max_levels);
        for i in (0..ask_count).rev() {
            let lvl = &s.depth_asks[i];
            let bar_len = (lvl.qty * 4.0).min(10.0) as usize;
            let bar: String = "\u{2591}".repeat(bar_len);
            lines.push(Line::from(vec![
                Span::styled(
                    format!("{:>12}", fmt_price_compact(lvl.price)),
                    Style::default().fg(ROSE),
                ),
                Span::styled(format!(" {:>6.3}", lvl.qty), Style::default().fg(TEXT_SEC)),
                Span::styled(format!(" {bar}"), Style::default().fg(ROSE)),
            ]));
        }

        // Spread line
        let spread = if !s.depth_asks.is_empty() && !s.depth_bids.is_empty() {
            let best_ask = s.depth_asks[0].price;
            let best_bid = s.depth_bids[0].price;
            let mid = (best_ask + best_bid) / 2.0;
            let sp_bps = if mid > 0.0 {
                (best_ask - best_bid) / mid * 10000.0
            } else {
                0.0
            };
            format!("{:.1}bp", sp_bps)
        } else {
            "-".to_string()
        };
        lines.push(Line::from(vec![
            Span::styled(
                format!("  \u{2500}\u{2500} spread {spread} "),
                Style::default().fg(TEXT_MUT),
            ),
            Span::styled(
                "\u{2500}\u{2500}\u{2500}\u{2500}\u{2500}",
                Style::default().fg(TEXT_MUT),
            ),
        ]));

        // Bids: show top N bids, highest first
        let bid_count = s.depth_bids.len().min(max_levels);
        for lvl in s.depth_bids.iter().take(bid_count) {
            let bar_len = (lvl.qty * 4.0).min(10.0) as usize;
            let bar: String = "\u{2588}".repeat(bar_len);
            lines.push(Line::from(vec![
                Span::styled(
                    format!("{:>12}", fmt_price_compact(lvl.price)),
                    Style::default().fg(GREEN),
                ),
                Span::styled(format!(" {:>6.3}", lvl.qty), Style::default().fg(TEXT_SEC)),
                Span::styled(format!(" {bar}"), Style::default().fg(GREEN)),
            ]));
        }
    }

    let paragraph = Paragraph::new(lines).block(
        Block::default()
            .title(Line::from(vec![
                Span::styled(
                    " BTC DEPTH ",
                    Style::default().fg(AMBER).add_modifier(Modifier::BOLD),
                ),
                Span::styled("\u{2588}", Style::default().fg(GREEN)),
                Span::styled("bid ", Style::default().fg(TEXT_MUT)),
                Span::styled("\u{2591}", Style::default().fg(ROSE)),
                Span::styled("ask ", Style::default().fg(TEXT_MUT)),
            ]))
            .borders(Borders::ALL)
            .border_style(Style::default().fg(BORDER))
            .padding(Padding::horizontal(1)),
    );
    frame.render_widget(paragraph, area);
}

// ── Latency (left column, bottom) ──────────────────────────────

fn draw_latency(frame: &mut ratatui::Frame, area: Rect, s: &DashState) {
    let chunks = Layout::vertical([
        Constraint::Length(3), // sparkline
        Constraint::Min(3),    // stats
    ])
    .split(area);

    let lat_data: Vec<u64> = s.latency_history.as_slice().iter().copied().collect();
    let sparkline = Sparkline::default()
        .data(&lat_data)
        .style(Style::default().fg(CYAN))
        .block(
            Block::default()
                .title(Line::from(Span::styled(
                    " LATENCY ",
                    Style::default().fg(AMBER).add_modifier(Modifier::BOLD),
                )))
                .borders(Borders::TOP | Borders::LEFT | Borders::RIGHT)
                .border_style(Style::default().fg(BORDER))
                .padding(Padding::horizontal(1)),
        );
    frame.render_widget(sparkline, chunks[0]);

    let lat = &s.latency;
    let wm_lag = if s.pipeline_watermark > 0 {
        let now_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as i64;
        format!(
            "{}ms",
            fmt_int(now_ms.saturating_sub(s.pipeline_watermark) as u64)
        )
    } else {
        "-".to_string()
    };

    let lines = vec![
        Line::from(vec![
            Span::styled("p50 ", Style::default().fg(TEXT_MUT)),
            Span::styled(fmt_latency(lat.p50()), Style::default().fg(CYAN)),
        ]),
        Line::from(vec![
            Span::styled("p99 ", Style::default().fg(TEXT_MUT)),
            Span::styled(fmt_latency(lat.p99()), Style::default().fg(AMBER)),
        ]),
        Line::from(vec![
            Span::styled("max ", Style::default().fg(TEXT_MUT)),
            Span::styled(fmt_latency(lat.max()), Style::default().fg(ROSE)),
        ]),
        Line::from(vec![
            Span::styled("tpt ", Style::default().fg(TEXT_MUT)),
            Span::styled(
                format!("{}/s", fmt_int(s.throughput as u64)),
                Style::default().fg(GREEN),
            ),
        ]),
        Line::from(vec![
            Span::styled("evt ", Style::default().fg(TEXT_MUT)),
            Span::styled(fmt_int(s.total_events), Style::default().fg(TEXT_PRI)),
        ]),
        Line::from(vec![
            Span::styled("wml ", Style::default().fg(TEXT_MUT)),
            Span::styled(wm_lag, Style::default().fg(AMBER)),
        ]),
    ];

    let stats = Paragraph::new(lines).block(
        Block::default()
            .borders(Borders::BOTTOM | Borders::LEFT | Borders::RIGHT)
            .border_style(Style::default().fg(BORDER))
            .padding(Padding::horizontal(1)),
    );
    frame.render_widget(stats, chunks[1]);
}

// ── Momentum heatmap (center, bottom) ──────────────────────────

fn draw_momentum(frame: &mut ratatui::Frame, area: Rect, s: &DashState) {
    let mut lines = Vec::new();

    for (i, sym) in SYMBOLS.iter().enumerate() {
        let key = sym.to_uppercase();

        if let Some(m) = s.momentum.get(&key) {
            let blocks = ((m.range_bps_30s / 5.0).min(7.0)) as usize;
            let filled: String = "\u{25a0}".repeat(blocks);
            let empty: String = "\u{25a1}".repeat(7 - blocks);

            let (sign, color) = if let Some(sig) = s.signals.get(&key) {
                match sig.signal.as_str() {
                    "BUY" => ("+", GREEN),
                    "SELL" => ("-", ROSE),
                    _ => (" ", TEXT_MUT),
                }
            } else {
                (" ", TEXT_MUT)
            };

            lines.push(Line::from(vec![
                Span::styled(
                    format!("{:>4} ", LABELS[i]),
                    Style::default()
                        .fg(SYMBOL_COLORS[i])
                        .add_modifier(Modifier::BOLD),
                ),
                Span::styled(filled, Style::default().fg(color)),
                Span::styled(empty, Style::default().fg(TEXT_MUT)),
                Span::styled(
                    format!(" {}{:.1}bp", sign, m.range_bps_30s),
                    Style::default().fg(color),
                ),
            ]));
        } else {
            lines.push(Line::from(Span::styled(
                format!("{:>4}  waiting...", LABELS[i]),
                Style::default().fg(TEXT_MUT),
            )));
        }
    }

    let paragraph = Paragraph::new(lines).block(
        Block::default()
            .title(Line::from(Span::styled(
                " MOMENTUM ",
                Style::default().fg(AMBER).add_modifier(Modifier::BOLD),
            )))
            .borders(Borders::ALL)
            .border_style(Style::default().fg(BORDER))
            .padding(Padding::horizontal(1)),
    );
    frame.render_widget(paragraph, area);
}

// ── Signals (center-right, bottom) ─────────────────────────────

fn draw_signals(frame: &mut ratatui::Frame, area: Rect, s: &DashState) {
    let mut lines = Vec::new();

    for (i, sym) in SYMBOLS.iter().enumerate() {
        let key = sym.to_uppercase();

        if let Some(sig) = s.signals.get(&key) {
            let (text, color) = match sig.signal.as_str() {
                "BUY" => ("BUY  \u{25cf}", GREEN),
                "SELL" => ("SELL \u{25cf}", ROSE),
                _ => ("HOLD  ", TEXT_MUT),
            };

            lines.push(Line::from(vec![
                Span::styled(
                    format!("{:>4} ", LABELS[i]),
                    Style::default()
                        .fg(SYMBOL_COLORS[i])
                        .add_modifier(Modifier::BOLD),
                ),
                Span::styled(
                    text,
                    Style::default().fg(color).add_modifier(Modifier::BOLD),
                ),
            ]));
        } else {
            lines.push(Line::from(Span::styled(
                format!("{:>4}  -", LABELS[i]),
                Style::default().fg(TEXT_MUT),
            )));
        }
    }

    let paragraph = Paragraph::new(lines).block(
        Block::default()
            .title(Line::from(Span::styled(
                " SIGNALS ",
                Style::default().fg(AMBER).add_modifier(Modifier::BOLD),
            )))
            .borders(Borders::ALL)
            .border_style(Style::default().fg(BORDER))
            .padding(Padding::horizontal(1)),
    );
    frame.render_widget(paragraph, area);
}

// ── Bursts (right, bottom) ─────────────────────────────────────

fn draw_bursts(frame: &mut ratatui::Frame, area: Rect, s: &DashState) {
    let mut lines = Vec::new();

    for (i, sym) in SYMBOLS.iter().enumerate() {
        let key = sym.to_uppercase();

        if let Some(b) = s.bursts.get(&key) {
            lines.push(Line::from(vec![
                Span::styled(
                    format!("{:>4} ", LABELS[i]),
                    Style::default()
                        .fg(SYMBOL_COLORS[i])
                        .add_modifier(Modifier::BOLD),
                ),
                Span::styled(format!("{:>4}t", b.burst_trades), Style::default().fg(CYAN)),
                Span::styled(
                    format!(" {:>5.1}bp", b.burst_range_bps),
                    Style::default().fg(AMBER),
                ),
            ]));
        } else {
            lines.push(Line::from(Span::styled(
                format!("{:>4}  -", LABELS[i]),
                Style::default().fg(TEXT_MUT),
            )));
        }
    }

    let paragraph = Paragraph::new(lines).block(
        Block::default()
            .title(Line::from(Span::styled(
                " BURSTS ",
                Style::default().fg(AMBER).add_modifier(Modifier::BOLD),
            )))
            .borders(Borders::ALL)
            .border_style(Style::default().fg(BORDER))
            .padding(Padding::horizontal(1)),
    );
    frame.render_widget(paragraph, area);
}

// ── Footer ─────────────────────────────────────────────────────

fn draw_footer(frame: &mut ratatui::Frame, area: Rect, s: &DashState) {
    let footer = Paragraph::new(Line::from(vec![
        Span::styled(" events: ", Style::default().fg(TEXT_MUT)),
        Span::styled(fmt_int(s.total_events), Style::default().fg(TEXT_PRI)),
        Span::styled("  emitted: ", Style::default().fg(TEXT_MUT)),
        Span::styled(fmt_int(s.total_emitted), Style::default().fg(GREEN)),
        Span::styled("  dropped: ", Style::default().fg(TEXT_MUT)),
        Span::styled(
            fmt_int(s.total_dropped),
            Style::default().fg(if s.total_dropped > 0 { ROSE } else { GREEN }),
        ),
        Span::styled(
            "                                         q: quit",
            Style::default().fg(TEXT_MUT),
        ),
    ]))
    .block(
        Block::default()
            .borders(Borders::TOP)
            .border_style(Style::default().fg(BORDER)),
    );
    frame.render_widget(footer, area);
}

// ── Formatting helpers ─────────────────────────────────────────

fn fmt_price_compact(v: f64) -> String {
    if v < 1.0 {
        return format!("${v:.4}");
    }
    if v < 10.0 {
        return format!("${v:.3}");
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

fn fmt_int(v: u64) -> String {
    let s = v.to_string();
    s.as_bytes()
        .rchunks(3)
        .rev()
        .map(|c| std::str::from_utf8(c).unwrap())
        .collect::<Vec<_>>()
        .join(",")
}

fn fmt_latency(ns: u64) -> String {
    if ns < 1_000 {
        format!("{ns}ns")
    } else if ns < 1_000_000 {
        format!("{:.1}us", ns as f64 / 1_000.0)
    } else if ns < 1_000_000_000 {
        format!("{:.1}ms", ns as f64 / 1_000_000.0)
    } else {
        format!("{:.2}s", ns as f64 / 1_000_000_000.0)
    }
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
