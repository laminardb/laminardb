//! CLI subcommands for interacting with a running LaminarDB server.

use anyhow::{Context, Result};

use crate::Command;

pub async fn run_command(command: Command) -> Result<()> {
    match command {
        Command::Status { server } => cmd_status(&server).await,
        Command::Sql { server, query } => cmd_sql(&server, &query).await,
        Command::Checkpoint { server } => cmd_checkpoint(&server).await,
        Command::Cluster { server } => cmd_cluster(&server).await,
    }
}

async fn cmd_status(server: &str) -> Result<()> {
    let url = format!("{server}/api/v1/diagnostics");
    let resp: serde_json::Value = reqwest::get(&url)
        .await
        .context("failed to connect to server")?
        .error_for_status()
        .context("server returned an error")?
        .json()
        .await
        .context("invalid response")?;
    println!("{}", serde_json::to_string_pretty(&resp)?);
    Ok(())
}

async fn cmd_sql(server: &str, query: &str) -> Result<()> {
    let url = format!("{server}/api/v1/sql");
    let client = reqwest::Client::new();
    let resp: serde_json::Value = client
        .post(&url)
        .json(&serde_json::json!({ "sql": query }))
        .send()
        .await
        .context("failed to connect to server")?
        .error_for_status()
        .context("server returned an error")?
        .json()
        .await
        .context("invalid response")?;
    println!("{}", serde_json::to_string_pretty(&resp)?);
    Ok(())
}

async fn cmd_checkpoint(server: &str) -> Result<()> {
    let url = format!("{server}/api/v1/checkpoint");
    let client = reqwest::Client::new();
    let resp: serde_json::Value = client
        .post(&url)
        .send()
        .await
        .context("failed to connect to server")?
        .error_for_status()
        .context("server returned an error")?
        .json()
        .await
        .context("invalid response")?;
    println!("{}", serde_json::to_string_pretty(&resp)?);
    Ok(())
}

async fn cmd_cluster(server: &str) -> Result<()> {
    let url = format!("{server}/api/v1/cluster");
    let resp: serde_json::Value = reqwest::get(&url)
        .await
        .context("failed to connect to server")?
        .error_for_status()
        .context("server returned an error")?
        .json()
        .await
        .context("invalid response")?;
    println!("{}", serde_json::to_string_pretty(&resp)?);
    Ok(())
}
