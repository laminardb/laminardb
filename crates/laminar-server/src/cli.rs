//! CLI subcommands for interacting with a running LaminarDB server.
//!
//! Each command issues HTTP requests to the server's REST API and
//! pretty-prints the results.

use anyhow::{Context, Result};

use crate::Command;

/// Dispatch a CLI subcommand.
pub async fn run_command(command: Command) -> Result<()> {
    match command {
        Command::Status { server } => cmd_status(&server).await,
        Command::Sql { query, server } => cmd_sql(&server, &query).await,
        Command::Checkpoint { server } => cmd_checkpoint(&server).await,
        Command::Cluster { server } => cmd_cluster(&server).await,
    }
}

/// `laminardb status` — show running sources, streams, and sinks.
async fn cmd_status(server: &str) -> Result<()> {
    let client = reqwest::Client::new();

    // Fetch diagnostics for overview
    let diag: serde_json::Value = client
        .get(format!("{server}/api/v1/diagnostics"))
        .send()
        .await
        .context("failed to connect to server")?
        .json()
        .await
        .context("failed to parse diagnostics response")?;

    println!(
        "LaminarDB {} ({})",
        diag["version"].as_str().unwrap_or("?"),
        diag["pipeline_state"].as_str().unwrap_or("?")
    );
    println!(
        "Uptime: {}s | Queries: {} | Events: ingested={}, emitted={}",
        diag["uptime_seconds"],
        diag["active_queries"],
        diag["counters"]["events_ingested"],
        diag["counters"]["events_emitted"],
    );
    println!();

    // Sources
    let sources: Vec<serde_json::Value> = client
        .get(format!("{server}/api/v1/sources"))
        .send()
        .await?
        .json()
        .await?;
    println!("Sources ({}):", sources.len());
    for s in &sources {
        println!("  - {}", s["name"].as_str().unwrap_or("?"));
    }

    // Streams
    let streams: Vec<serde_json::Value> = client
        .get(format!("{server}/api/v1/streams"))
        .send()
        .await?
        .json()
        .await?;
    println!("Streams ({}):", streams.len());
    for s in &streams {
        let name = s["name"].as_str().unwrap_or("?");
        if let Some(sql) = s["sql"].as_str() {
            println!("  - {name}: {sql}");
        } else {
            println!("  - {name}");
        }
    }

    // Sinks
    let sinks: Vec<serde_json::Value> = client
        .get(format!("{server}/api/v1/sinks"))
        .send()
        .await?
        .json()
        .await?;
    println!("Sinks ({}):", sinks.len());
    for s in &sinks {
        println!("  - {}", s["name"].as_str().unwrap_or("?"));
    }

    Ok(())
}

/// `laminardb sql "..."` — execute a SQL query.
async fn cmd_sql(server: &str, query: &str) -> Result<()> {
    let client = reqwest::Client::new();
    let resp = client
        .post(format!("{server}/api/v1/sql"))
        .json(&serde_json::json!({ "sql": query }))
        .send()
        .await
        .context("failed to connect to server")?;

    let status = resp.status();
    let body: serde_json::Value = resp.json().await.context("failed to parse response")?;

    if status.is_success() {
        println!("{}", serde_json::to_string_pretty(&body)?);
    } else {
        let msg = body["error"].as_str().unwrap_or("unknown error");
        anyhow::bail!("SQL error ({}): {}", status, msg);
    }

    Ok(())
}

/// `laminardb checkpoint` — trigger a manual checkpoint.
async fn cmd_checkpoint(server: &str) -> Result<()> {
    let client = reqwest::Client::new();
    let resp = client
        .post(format!("{server}/api/v1/checkpoint"))
        .send()
        .await
        .context("failed to connect to server")?;

    let body: serde_json::Value = resp.json().await.context("failed to parse response")?;

    if body["success"].as_bool() == Some(true) {
        println!(
            "Checkpoint completed: id={}, epoch={}, duration={}ms",
            body["checkpoint_id"], body["epoch"], body["duration_ms"]
        );
    } else {
        let err = body["error"].as_str().unwrap_or("unknown error");
        anyhow::bail!("Checkpoint failed: {}", err);
    }

    Ok(())
}

/// `laminardb cluster` — show cluster status (delta mode).
async fn cmd_cluster(server: &str) -> Result<()> {
    let client = reqwest::Client::new();
    let resp = client
        .get(format!("{server}/api/v1/cluster"))
        .send()
        .await
        .context("failed to connect to server")?;

    let status = resp.status();
    let body: serde_json::Value = resp.json().await.context("failed to parse response")?;

    if status.is_success() {
        println!("{}", serde_json::to_string_pretty(&body)?);
    } else {
        let msg = body["error"].as_str().unwrap_or("unknown error");
        anyhow::bail!("Cluster status error ({}): {}", status, msg);
    }

    Ok(())
}
