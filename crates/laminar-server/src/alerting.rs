//! Alerting subsystem for LaminarDB server.
//!
//! Evaluates configurable alert rules on a periodic interval and dispatches
//! notifications through multiple channels (webhook, log, file).
//!
//! # Architecture
//!
//! The [`AlertManager`] runs a background evaluation loop that:
//! 1. Reads current metrics from the `LaminarDB` instance
//! 2. Evaluates each [`AlertRule`] condition against those metrics
//! 3. Respects per-rule cooldown to prevent alert storms
//! 4. Dispatches alerts through configured [`AlertChannel`]s
//! 5. Maintains a ring buffer of recent alert history

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant};

use serde::{Deserialize, Serialize};
use tokio::sync::RwLock;
use tracing::{error, info, warn};

use laminar_db::LaminarDB;

// ---------------------------------------------------------------------------
// Configuration types
// ---------------------------------------------------------------------------

/// Configuration for a single alert rule, deserialized from `[[alert]]` TOML.
#[derive(Debug, Clone, PartialEq, Deserialize)]
pub struct AlertConfig {
    /// Human-readable alert name.
    pub name: String,

    /// Condition expression (e.g., `"source_lag_seconds > 30"`).
    pub condition: String,

    /// Channel names to dispatch to (e.g., `["webhook", "log"]`).
    #[serde(default)]
    pub channels: Vec<String>,

    /// Minimum time between firings of this rule.
    #[serde(default = "default_cooldown", with = "humantime_serde")]
    pub cooldown: Duration,

    /// Webhook configuration (used when "webhook" is in `channels`).
    #[serde(default)]
    pub webhook: Option<WebhookConfig>,

    /// File path for file channel (used when "file" is in `channels`).
    #[serde(default)]
    pub file: Option<AlertFileConfig>,
}

/// Webhook delivery configuration.
#[derive(Debug, Clone, PartialEq, Deserialize)]
pub struct WebhookConfig {
    /// Target URL.
    pub url: String,

    /// HTTP method (default: POST).
    #[serde(default = "default_method")]
    pub method: String,

    /// Additional HTTP headers.
    #[serde(default)]
    pub headers: HashMap<String, String>,

    /// Body template with `{{name}}` and `{{message}}` placeholders.
    #[serde(default)]
    pub template: Option<String>,
}

/// File channel configuration.
#[derive(Debug, Clone, PartialEq, Deserialize)]
pub struct AlertFileConfig {
    /// Path to append alert lines to.
    pub path: String,
}

fn default_cooldown() -> Duration {
    Duration::from_secs(300) // 5 minutes
}

fn default_method() -> String {
    "POST".to_string()
}

// ---------------------------------------------------------------------------
// Parsed rule representation
// ---------------------------------------------------------------------------

/// A parsed, evaluable alert condition.
#[derive(Debug, Clone)]
pub enum AlertCondition {
    /// `source_lag_seconds > threshold`
    SourceLagAbove(f64),
    /// `checkpoint_failed` — true when failed count increases.
    CheckpointFailed,
    /// `sink_error_count > threshold`
    SinkErrorAbove(u64),
    /// `throughput_below(events_per_sec)` — ingestion rate below threshold.
    ThroughputBelow(f64),
    /// `node_down` — pipeline not in Running state (useful in delta mode).
    NodeDown,
}

/// A fully resolved alert rule ready for evaluation.
#[derive(Debug, Clone)]
pub struct AlertRule {
    /// Rule name.
    pub name: String,
    /// Parsed condition.
    pub condition: AlertCondition,
    /// Channels to dispatch to.
    pub channels: Vec<AlertChannel>,
    /// Cooldown between firings.
    pub cooldown: Duration,
}

/// An alert dispatch channel.
#[derive(Debug, Clone)]
pub enum AlertChannel {
    /// Structured log event (always available).
    Log,
    /// HTTP webhook.
    Webhook {
        /// Target URL.
        url: String,
        /// HTTP method.
        method: String,
        /// Extra headers.
        headers: HashMap<String, String>,
        /// Body template.
        template: Option<String>,
    },
    /// Append to file.
    File {
        /// File path.
        path: PathBuf,
    },
}

// ---------------------------------------------------------------------------
// Alert event
// ---------------------------------------------------------------------------

/// A fired alert event.
#[derive(Debug, Clone, Serialize)]
pub struct AlertEvent {
    /// Rule name that triggered.
    pub name: String,
    /// Human-readable message.
    pub message: String,
    /// ISO 8601 timestamp.
    pub fired_at: String,
    /// Severity level.
    pub severity: &'static str,
}

// ---------------------------------------------------------------------------
// Parsing
// ---------------------------------------------------------------------------

/// Parse a condition string into an `AlertCondition`.
///
/// Supported formats:
/// - `source_lag_seconds > N`
/// - `checkpoint_failed`
/// - `sink_error_count > N`
/// - `throughput_below(N)`
/// - `node_down`
pub fn parse_condition(input: &str) -> Result<AlertCondition, AlertError> {
    let s = input.trim();

    if s == "checkpoint_failed" {
        return Ok(AlertCondition::CheckpointFailed);
    }
    if s == "node_down" {
        return Ok(AlertCondition::NodeDown);
    }

    // source_lag_seconds > N
    if let Some(rest) = s.strip_prefix("source_lag_seconds") {
        let rest = rest.trim();
        if let Some(val) = rest.strip_prefix('>') {
            let threshold: f64 = val
                .trim()
                .parse()
                .map_err(|_| AlertError::InvalidCondition {
                    condition: input.to_string(),
                    reason: "invalid threshold value for source_lag_seconds".to_string(),
                })?;
            return Ok(AlertCondition::SourceLagAbove(threshold));
        }
    }

    // sink_error_count > N
    if let Some(rest) = s.strip_prefix("sink_error_count") {
        let rest = rest.trim();
        if let Some(val) = rest.strip_prefix('>') {
            let threshold: u64 = val
                .trim()
                .parse()
                .map_err(|_| AlertError::InvalidCondition {
                    condition: input.to_string(),
                    reason: "invalid threshold value for sink_error_count".to_string(),
                })?;
            return Ok(AlertCondition::SinkErrorAbove(threshold));
        }
    }

    // throughput_below(N)
    if let Some(rest) = s.strip_prefix("throughput_below(") {
        if let Some(val) = rest.strip_suffix(')') {
            let threshold: f64 = val
                .trim()
                .parse()
                .map_err(|_| AlertError::InvalidCondition {
                    condition: input.to_string(),
                    reason: "invalid threshold value for throughput_below".to_string(),
                })?;
            return Ok(AlertCondition::ThroughputBelow(threshold));
        }
    }

    Err(AlertError::InvalidCondition {
        condition: input.to_string(),
        reason: "unrecognized condition syntax".to_string(),
    })
}

/// Build an `AlertRule` from TOML config.
pub fn build_rule(config: &AlertConfig) -> Result<AlertRule, AlertError> {
    let condition = parse_condition(&config.condition)?;

    let mut channels = Vec::new();
    for ch_name in &config.channels {
        match ch_name.as_str() {
            "log" => channels.push(AlertChannel::Log),
            "webhook" => {
                let wh = config
                    .webhook
                    .as_ref()
                    .ok_or_else(|| AlertError::InvalidCondition {
                        condition: config.name.clone(),
                        reason: "webhook channel requires [alert.webhook] config".to_string(),
                    })?;
                channels.push(AlertChannel::Webhook {
                    url: wh.url.clone(),
                    method: wh.method.clone(),
                    headers: wh.headers.clone(),
                    template: wh.template.clone(),
                });
            }
            "file" => {
                let fc = config
                    .file
                    .as_ref()
                    .ok_or_else(|| AlertError::InvalidCondition {
                        condition: config.name.clone(),
                        reason: "file channel requires [alert.file] config".to_string(),
                    })?;
                channels.push(AlertChannel::File {
                    path: PathBuf::from(&fc.path),
                });
            }
            other => {
                return Err(AlertError::InvalidCondition {
                    condition: config.name.clone(),
                    reason: format!("unknown channel: '{other}'"),
                });
            }
        }
    }

    // Always include log channel if no channels specified
    if channels.is_empty() {
        channels.push(AlertChannel::Log);
    }

    Ok(AlertRule {
        name: config.name.clone(),
        condition,
        channels,
        cooldown: config.cooldown,
    })
}

// ---------------------------------------------------------------------------
// Metrics snapshot for evaluation
// ---------------------------------------------------------------------------

/// Snapshot of metrics used for alert condition evaluation.
#[derive(Debug, Default)]
struct MetricsSnapshot {
    /// Total events ingested (for throughput calculation).
    events_ingested: u64,
    /// Number of failed checkpoints.
    checkpoints_failed: u64,
    /// Pipeline state string.
    pipeline_state: &'static str,
    /// Total events dropped (proxy for sink/pipeline errors).
    events_dropped: u64,
}

// ---------------------------------------------------------------------------
// AlertManager
// ---------------------------------------------------------------------------

/// Manages alert rule evaluation, cooldown tracking, and dispatch.
pub struct AlertManager {
    /// Configured alert rules.
    rules: Vec<AlertRule>,
    /// Evaluation interval.
    interval: Duration,
    /// Per-rule last-fired instant for cooldown enforcement.
    last_fired: RwLock<HashMap<String, Instant>>,
    /// Ring buffer of recent alerts (most recent last).
    history: RwLock<Vec<AlertEvent>>,
    /// Maximum history size.
    history_capacity: usize,
    /// HTTP client for webhook delivery.
    http_client: reqwest::Client,
    /// Previous metrics snapshot for delta comparisons.
    prev_snapshot: RwLock<Option<(Instant, MetricsSnapshot)>>,
}

impl AlertManager {
    /// Create a new `AlertManager` from a list of alert configs.
    ///
    /// Invalid rules are logged and skipped.
    pub fn new(configs: &[AlertConfig], interval: Duration) -> Self {
        let mut rules = Vec::new();
        for config in configs {
            match build_rule(config) {
                Ok(rule) => {
                    info!(name = %rule.name, "Loaded alert rule");
                    rules.push(rule);
                }
                Err(e) => {
                    warn!(name = %config.name, error = %e, "Skipping invalid alert rule");
                }
            }
        }

        Self {
            rules,
            interval,
            last_fired: RwLock::new(HashMap::new()),
            history: RwLock::new(Vec::new()),
            history_capacity: 100,
            http_client: reqwest::Client::new(),
            prev_snapshot: RwLock::new(None),
        }
    }

    /// Return the list of configured rule summaries.
    pub fn rules_summary(&self) -> Vec<AlertRuleSummary> {
        self.rules
            .iter()
            .map(|r| AlertRuleSummary {
                name: r.name.clone(),
                condition: format!("{:?}", r.condition),
                channels: r
                    .channels
                    .iter()
                    .map(|c| match c {
                        AlertChannel::Log => "log".to_string(),
                        AlertChannel::Webhook { .. } => "webhook".to_string(),
                        AlertChannel::File { .. } => "file".to_string(),
                    })
                    .collect(),
                cooldown_secs: r.cooldown.as_secs(),
            })
            .collect()
    }

    /// Return recent alert history.
    pub async fn recent_alerts(&self) -> Vec<AlertEvent> {
        self.history.read().await.clone()
    }

    /// Fire a test alert through all channels of all rules.
    pub async fn fire_test_alert(&self) {
        let event = AlertEvent {
            name: "test".to_string(),
            message: "This is a test alert from LaminarDB".to_string(),
            fired_at: chrono::Utc::now().to_rfc3339(),
            severity: "info",
        };

        for rule in &self.rules {
            for channel in &rule.channels {
                self.dispatch(&event, channel).await;
            }
        }

        // Record in history
        let mut history = self.history.write().await;
        if history.len() >= self.history_capacity {
            history.remove(0);
        }
        history.push(event);
    }

    /// Run the evaluation loop. This is meant to be spawned as a background task.
    pub async fn run_loop(self: Arc<Self>, db: Arc<LaminarDB>) {
        let mut ticker = tokio::time::interval(self.interval);
        // Skip the first immediate tick
        ticker.tick().await;

        loop {
            ticker.tick().await;
            self.evaluate(&db).await;
        }
    }

    /// Evaluate all rules once against current metrics.
    async fn evaluate(&self, db: &LaminarDB) {
        let metrics = db.metrics();
        let counters = db.counters().snapshot();
        let pipeline_state = db.pipeline_state();

        let snapshot = MetricsSnapshot {
            events_ingested: metrics.total_events_ingested,
            checkpoints_failed: counters.checkpoints_failed,
            pipeline_state,
            events_dropped: metrics.total_events_dropped,
        };

        let now = Instant::now();

        for rule in &self.rules {
            // Check cooldown
            {
                let last_fired = self.last_fired.read().await;
                if let Some(last) = last_fired.get(&rule.name) {
                    if now.duration_since(*last) < rule.cooldown {
                        continue;
                    }
                }
            }

            let triggered = self.check_condition(&rule.condition, &snapshot, now).await;

            if triggered {
                let message = self.format_message(&rule.condition, &snapshot);
                let event = AlertEvent {
                    name: rule.name.clone(),
                    message,
                    fired_at: chrono::Utc::now().to_rfc3339(),
                    severity: "warning",
                };

                // Dispatch to all channels
                for channel in &rule.channels {
                    self.dispatch(&event, channel).await;
                }

                // Update cooldown
                {
                    let mut last_fired = self.last_fired.write().await;
                    last_fired.insert(rule.name.clone(), now);
                }

                // Record in history
                {
                    let mut history = self.history.write().await;
                    if history.len() >= self.history_capacity {
                        history.remove(0);
                    }
                    history.push(event);
                }
            }
        }

        // Store snapshot for next evaluation
        let mut prev = self.prev_snapshot.write().await;
        *prev = Some((now, snapshot));
    }

    /// Check whether a condition is currently triggered.
    async fn check_condition(
        &self,
        condition: &AlertCondition,
        snapshot: &MetricsSnapshot,
        now: Instant,
    ) -> bool {
        match condition {
            AlertCondition::SourceLagAbove(_threshold) => {
                // Source lag requires per-source lag tracking which isn't exposed
                // in the public metrics API yet. For now, estimate from throughput
                // stalls: if ingestion has been zero for the threshold seconds, fire.
                // This is a conservative approximation.
                let prev = self.prev_snapshot.read().await;
                if let Some((prev_time, prev_snap)) = prev.as_ref() {
                    let elapsed = now.duration_since(*prev_time).as_secs_f64();
                    let delta = snapshot
                        .events_ingested
                        .saturating_sub(prev_snap.events_ingested);
                    // If no events ingested in the evaluation window, consider it "lagging"
                    elapsed > 0.0 && delta == 0
                } else {
                    false
                }
            }
            AlertCondition::CheckpointFailed => {
                let prev = self.prev_snapshot.read().await;
                if let Some((_, prev_snap)) = prev.as_ref() {
                    snapshot.checkpoints_failed > prev_snap.checkpoints_failed
                } else {
                    snapshot.checkpoints_failed > 0
                }
            }
            AlertCondition::SinkErrorAbove(threshold) => snapshot.events_dropped > *threshold,
            AlertCondition::ThroughputBelow(threshold) => {
                let prev = self.prev_snapshot.read().await;
                if let Some((prev_time, prev_snap)) = prev.as_ref() {
                    let elapsed = now.duration_since(*prev_time).as_secs_f64();
                    if elapsed > 0.0 {
                        let delta = snapshot
                            .events_ingested
                            .saturating_sub(prev_snap.events_ingested);
                        let rate = delta as f64 / elapsed;
                        rate < *threshold
                    } else {
                        false
                    }
                } else {
                    false
                }
            }
            AlertCondition::NodeDown => snapshot.pipeline_state != "Running",
        }
    }

    /// Format a human-readable alert message.
    fn format_message(&self, condition: &AlertCondition, snapshot: &MetricsSnapshot) -> String {
        match condition {
            AlertCondition::SourceLagAbove(threshold) => {
                format!("Source lag exceeds {threshold}s threshold (no events ingested in evaluation window)")
            }
            AlertCondition::CheckpointFailed => {
                format!(
                    "Checkpoint failure detected (total failures: {})",
                    snapshot.checkpoints_failed
                )
            }
            AlertCondition::SinkErrorAbove(threshold) => {
                format!(
                    "Sink error count {} exceeds threshold {}",
                    snapshot.events_dropped, threshold
                )
            }
            AlertCondition::ThroughputBelow(threshold) => {
                format!("Throughput below {threshold} events/sec")
            }
            AlertCondition::NodeDown => {
                format!("Node is down (pipeline state: {})", snapshot.pipeline_state)
            }
        }
    }

    /// Dispatch an alert event to a channel.
    async fn dispatch(&self, event: &AlertEvent, channel: &AlertChannel) {
        match channel {
            AlertChannel::Log => {
                warn!(
                    alert_name = %event.name,
                    message = %event.message,
                    severity = %event.severity,
                    "Alert fired"
                );
            }
            AlertChannel::Webhook {
                url,
                method,
                headers,
                template,
            } => {
                let body = if let Some(tmpl) = template {
                    render_template(tmpl, event)
                } else {
                    serde_json::to_string(event).unwrap_or_default()
                };

                let mut req = match method.to_uppercase().as_str() {
                    "PUT" => self.http_client.put(url),
                    _ => self.http_client.post(url),
                };

                for (k, v) in headers {
                    req = req.header(k.as_str(), v.as_str());
                }

                // Default content-type if not set
                if !headers
                    .keys()
                    .any(|k| k.eq_ignore_ascii_case("content-type"))
                {
                    req = req.header("content-type", "application/json");
                }

                match req.body(body).send().await {
                    Ok(resp) => {
                        if !resp.status().is_success() {
                            warn!(
                                url = %url,
                                status = %resp.status(),
                                "Webhook alert delivery failed"
                            );
                        }
                    }
                    Err(e) => {
                        error!(url = %url, error = %e, "Webhook alert delivery error");
                    }
                }
            }
            AlertChannel::File { path } => {
                let line = format!(
                    "[{}] {} severity={} {}\n",
                    event.fired_at, event.name, event.severity, event.message
                );
                if let Err(e) = append_to_file(path, &line).await {
                    error!(path = %path.display(), error = %e, "Failed to write alert to file");
                }
            }
        }
    }
}

/// Render a template string with `{{name}}` and `{{message}}` placeholders.
pub fn render_template(template: &str, event: &AlertEvent) -> String {
    template
        .replace("{{name}}", &event.name)
        .replace("{{message}}", &event.message)
        .replace("{{severity}}", event.severity)
        .replace("{{fired_at}}", &event.fired_at)
}

/// Append a line to a file (async via tokio::fs).
async fn append_to_file(path: &std::path::Path, content: &str) -> std::io::Result<()> {
    use tokio::io::AsyncWriteExt;
    let mut file = tokio::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)
        .await?;
    file.write_all(content.as_bytes()).await?;
    file.flush().await?;
    Ok(())
}

// ---------------------------------------------------------------------------
// API response types
// ---------------------------------------------------------------------------

/// Summary of an alert rule for the API.
#[derive(Debug, Serialize)]
pub struct AlertRuleSummary {
    /// Rule name.
    pub name: String,
    /// Condition description.
    pub condition: String,
    /// Channel names.
    pub channels: Vec<String>,
    /// Cooldown in seconds.
    pub cooldown_secs: u64,
}

// ---------------------------------------------------------------------------
// Errors
// ---------------------------------------------------------------------------

/// Alert subsystem errors.
#[derive(Debug, thiserror::Error)]
pub enum AlertError {
    /// Invalid or unparseable alert condition.
    #[error("invalid alert condition '{condition}': {reason}")]
    InvalidCondition {
        /// The condition string.
        condition: String,
        /// Why it's invalid.
        reason: String,
    },
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    // -- Condition parsing tests --

    #[test]
    fn test_parse_source_lag() {
        let c = parse_condition("source_lag_seconds > 30").unwrap();
        assert!(matches!(c, AlertCondition::SourceLagAbove(v) if (v - 30.0).abs() < f64::EPSILON));
    }

    #[test]
    fn test_parse_source_lag_float() {
        let c = parse_condition("source_lag_seconds > 5.5").unwrap();
        assert!(matches!(c, AlertCondition::SourceLagAbove(v) if (v - 5.5).abs() < f64::EPSILON));
    }

    #[test]
    fn test_parse_checkpoint_failed() {
        let c = parse_condition("checkpoint_failed").unwrap();
        assert!(matches!(c, AlertCondition::CheckpointFailed));
    }

    #[test]
    fn test_parse_sink_error_count() {
        let c = parse_condition("sink_error_count > 10").unwrap();
        assert!(matches!(c, AlertCondition::SinkErrorAbove(10)));
    }

    #[test]
    fn test_parse_throughput_below() {
        let c = parse_condition("throughput_below(1000)").unwrap();
        assert!(
            matches!(c, AlertCondition::ThroughputBelow(v) if (v - 1000.0).abs() < f64::EPSILON)
        );
    }

    #[test]
    fn test_parse_throughput_below_float() {
        let c = parse_condition("throughput_below(99.5)").unwrap();
        assert!(matches!(c, AlertCondition::ThroughputBelow(v) if (v - 99.5).abs() < f64::EPSILON));
    }

    #[test]
    fn test_parse_node_down() {
        let c = parse_condition("node_down").unwrap();
        assert!(matches!(c, AlertCondition::NodeDown));
    }

    #[test]
    fn test_parse_invalid_condition() {
        let err = parse_condition("unknown_metric > 5").unwrap_err();
        assert!(err.to_string().contains("unrecognized"));
    }

    #[test]
    fn test_parse_invalid_threshold() {
        let err = parse_condition("source_lag_seconds > abc").unwrap_err();
        assert!(err.to_string().contains("invalid threshold"));
    }

    #[test]
    fn test_parse_whitespace_trimming() {
        let c = parse_condition("  checkpoint_failed  ").unwrap();
        assert!(matches!(c, AlertCondition::CheckpointFailed));
    }

    // -- Template rendering tests --

    #[test]
    fn test_render_template_basic() {
        let event = AlertEvent {
            name: "high-lag".to_string(),
            message: "Source lag exceeds 30s".to_string(),
            fired_at: "2026-01-01T00:00:00Z".to_string(),
            severity: "warning",
        };
        let template = r#"{"text": "Alert: {{name}} - {{message}}"}"#;
        let rendered = render_template(template, &event);
        assert_eq!(
            rendered,
            r#"{"text": "Alert: high-lag - Source lag exceeds 30s"}"#
        );
    }

    #[test]
    fn test_render_template_all_placeholders() {
        let event = AlertEvent {
            name: "test".to_string(),
            message: "msg".to_string(),
            fired_at: "2026-01-01T00:00:00Z".to_string(),
            severity: "warning",
        };
        let template = "{{name}}|{{message}}|{{severity}}|{{fired_at}}";
        let rendered = render_template(template, &event);
        assert_eq!(rendered, "test|msg|warning|2026-01-01T00:00:00Z");
    }

    #[test]
    fn test_render_template_no_placeholders() {
        let event = AlertEvent {
            name: "test".to_string(),
            message: "msg".to_string(),
            fired_at: "2026-01-01T00:00:00Z".to_string(),
            severity: "info",
        };
        let rendered = render_template("static text", &event);
        assert_eq!(rendered, "static text");
    }

    // -- build_rule tests --

    #[test]
    fn test_build_rule_log_channel() {
        let config = AlertConfig {
            name: "test-rule".to_string(),
            condition: "checkpoint_failed".to_string(),
            channels: vec!["log".to_string()],
            cooldown: Duration::from_secs(60),
            webhook: None,
            file: None,
        };
        let rule = build_rule(&config).unwrap();
        assert_eq!(rule.name, "test-rule");
        assert_eq!(rule.channels.len(), 1);
        assert!(matches!(rule.channels[0], AlertChannel::Log));
    }

    #[test]
    fn test_build_rule_webhook_channel() {
        let config = AlertConfig {
            name: "wh-rule".to_string(),
            condition: "node_down".to_string(),
            channels: vec!["webhook".to_string()],
            cooldown: Duration::from_secs(60),
            webhook: Some(WebhookConfig {
                url: "https://example.com/hook".to_string(),
                method: "POST".to_string(),
                headers: HashMap::new(),
                template: Some(r#"{"alert":"{{name}}"}"#.to_string()),
            }),
            file: None,
        };
        let rule = build_rule(&config).unwrap();
        assert!(
            matches!(&rule.channels[0], AlertChannel::Webhook { url, .. } if url == "https://example.com/hook")
        );
    }

    #[test]
    fn test_build_rule_webhook_missing_config() {
        let config = AlertConfig {
            name: "bad".to_string(),
            condition: "node_down".to_string(),
            channels: vec!["webhook".to_string()],
            cooldown: Duration::from_secs(60),
            webhook: None,
            file: None,
        };
        let err = build_rule(&config).unwrap_err();
        assert!(err.to_string().contains("webhook channel requires"));
    }

    #[test]
    fn test_build_rule_file_channel() {
        let config = AlertConfig {
            name: "file-rule".to_string(),
            condition: "checkpoint_failed".to_string(),
            channels: vec!["file".to_string()],
            cooldown: Duration::from_secs(60),
            webhook: None,
            file: Some(AlertFileConfig {
                path: "/tmp/alerts.log".to_string(),
            }),
        };
        let rule = build_rule(&config).unwrap();
        assert!(
            matches!(&rule.channels[0], AlertChannel::File { path } if path == std::path::Path::new("/tmp/alerts.log"))
        );
    }

    #[test]
    fn test_build_rule_empty_channels_defaults_to_log() {
        let config = AlertConfig {
            name: "default-ch".to_string(),
            condition: "node_down".to_string(),
            channels: vec![],
            cooldown: Duration::from_secs(60),
            webhook: None,
            file: None,
        };
        let rule = build_rule(&config).unwrap();
        assert_eq!(rule.channels.len(), 1);
        assert!(matches!(rule.channels[0], AlertChannel::Log));
    }

    #[test]
    fn test_build_rule_unknown_channel() {
        let config = AlertConfig {
            name: "bad-ch".to_string(),
            condition: "node_down".to_string(),
            channels: vec!["sms".to_string()],
            cooldown: Duration::from_secs(60),
            webhook: None,
            file: None,
        };
        let err = build_rule(&config).unwrap_err();
        assert!(err.to_string().contains("unknown channel: 'sms'"));
    }

    #[test]
    fn test_build_rule_multiple_channels() {
        let config = AlertConfig {
            name: "multi".to_string(),
            condition: "checkpoint_failed".to_string(),
            channels: vec!["log".to_string(), "file".to_string()],
            cooldown: Duration::from_secs(60),
            webhook: None,
            file: Some(AlertFileConfig {
                path: "/tmp/alerts.log".to_string(),
            }),
        };
        let rule = build_rule(&config).unwrap();
        assert_eq!(rule.channels.len(), 2);
        assert!(matches!(rule.channels[0], AlertChannel::Log));
        assert!(matches!(rule.channels[1], AlertChannel::File { .. }));
    }

    // -- TOML parsing tests --

    #[test]
    fn test_alert_config_toml_parsing() {
        let toml_str = r#"
name = "high-lag"
condition = "source_lag_seconds > 30"
channels = ["webhook", "log"]
cooldown = "5m"

[webhook]
url = "https://hooks.slack.com/services/..."
method = "POST"
template = '{"text": "Alert: {{name}} — {{message}}"}'

[webhook.headers]
"Content-Type" = "application/json"
"#;
        let config: AlertConfig = toml::from_str(toml_str).unwrap();
        assert_eq!(config.name, "high-lag");
        assert_eq!(config.condition, "source_lag_seconds > 30");
        assert_eq!(config.channels.len(), 2);
        assert_eq!(config.cooldown, Duration::from_secs(300));
        assert!(config.webhook.is_some());
        let wh = config.webhook.unwrap();
        assert_eq!(wh.method, "POST");
        assert!(wh.template.unwrap().contains("{{name}}"));
    }

    #[test]
    fn test_alert_config_minimal_toml() {
        let toml_str = r#"
name = "cp-fail"
condition = "checkpoint_failed"
"#;
        let config: AlertConfig = toml::from_str(toml_str).unwrap();
        assert_eq!(config.name, "cp-fail");
        assert!(config.channels.is_empty());
        assert_eq!(config.cooldown, Duration::from_secs(300)); // default
        assert!(config.webhook.is_none());
    }

    // -- AlertManager construction tests --

    #[test]
    fn test_alert_manager_new() {
        let configs = vec![
            AlertConfig {
                name: "r1".to_string(),
                condition: "node_down".to_string(),
                channels: vec!["log".to_string()],
                cooldown: Duration::from_secs(60),
                webhook: None,
                file: None,
            },
            AlertConfig {
                name: "r2".to_string(),
                condition: "invalid_garbage".to_string(),
                channels: vec![],
                cooldown: Duration::from_secs(60),
                webhook: None,
                file: None,
            },
        ];
        let mgr = AlertManager::new(&configs, Duration::from_secs(10));
        // r1 should load, r2 should be skipped
        assert_eq!(mgr.rules.len(), 1);
        assert_eq!(mgr.rules[0].name, "r1");
    }

    #[test]
    fn test_alert_manager_rules_summary() {
        let configs = vec![AlertConfig {
            name: "lag".to_string(),
            condition: "source_lag_seconds > 30".to_string(),
            channels: vec!["log".to_string()],
            cooldown: Duration::from_secs(300),
            webhook: None,
            file: None,
        }];
        let mgr = AlertManager::new(&configs, Duration::from_secs(10));
        let summaries = mgr.rules_summary();
        assert_eq!(summaries.len(), 1);
        assert_eq!(summaries[0].name, "lag");
        assert_eq!(summaries[0].channels, vec!["log"]);
        assert_eq!(summaries[0].cooldown_secs, 300);
    }

    // -- Cooldown tests --

    #[tokio::test]
    async fn test_cooldown_tracking() {
        let mgr = AlertManager::new(&[], Duration::from_secs(10));
        let name = "test-rule".to_string();

        // No previous firing → should not be in cooldown
        let last_fired = mgr.last_fired.read().await;
        assert!(!last_fired.contains_key(&name));
        drop(last_fired);

        // Simulate firing
        let mut last_fired = mgr.last_fired.write().await;
        last_fired.insert(name.clone(), Instant::now());
        drop(last_fired);

        // Now should be in cooldown
        let last_fired = mgr.last_fired.read().await;
        assert!(last_fired.contains_key(&name));
    }

    // -- File append test --

    #[tokio::test]
    async fn test_append_to_file() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("alerts.log");

        append_to_file(&path, "line 1\n").await.unwrap();
        append_to_file(&path, "line 2\n").await.unwrap();

        let content = tokio::fs::read_to_string(&path).await.unwrap();
        assert_eq!(content, "line 1\nline 2\n");
    }
}
