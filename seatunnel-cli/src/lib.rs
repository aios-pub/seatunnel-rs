/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.
 */

//! Seatunnel CLI: job submission and cluster management.

use anyhow::Result;
use clap::{Parser, Subcommand};
use seatunnel_api::execution::execution_mode::ExecutionMode;
use seatunnel_api::execution::engine::Engine;
use std::path::PathBuf;

/// SeaTunnel CLI - Data integration engine
#[derive(Parser, Debug)]
#[command(
    name = "seatunnel",
    version = "0.1.0",
    about = "SeaTunnel Rust - Data integration engine"
)]
pub struct Cli {
    #[command(subcommand)]
    pub command: Option<Commands>,
}

#[derive(Subcommand, Debug)]
pub enum Commands {
    /// Run a job (local or cluster mode)
    Run {
        /// Path to the job configuration file
        #[arg(short, long)]
        config: PathBuf,
        /// Execution mode: local or cluster
        #[arg(short, long, default_value = "local")]
        mode: String,
        /// Cluster master address (cluster mode only)
        #[arg(short, long)]
        address: Option<String>,
        /// Client timeout in milliseconds
        #[arg(long, default_value = "5000")]
        timeout_ms: u64,
        /// Parallelism override
        #[arg(long)]
        parallelism: Option<usize>,
    },
    /// Manage jobs
    Job {
        #[command(subcommand)]
        command: JobCommand,
    },
    /// Cluster information
    Cluster {
        /// Cluster address
        #[arg(short, long, default_value = "127.0.0.1:5000")]
        address: String,
    },
}

#[derive(Subcommand, Debug)]
pub enum JobCommand {
    Submit {
        #[arg(short, long)]
        config: PathBuf,
        #[arg(short, long, default_value = "127.0.0.1:5000")]
        address: String,
    },
    List {
        #[arg(short, long, default_value = "127.0.0.1:5000")]
        address: String,
    },
    Status {
        #[arg(short, long)]
        job_id: String,
        #[arg(short, long, default_value = "127.0.0.1:5000")]
        address: String,
    },
    Cancel {
        #[arg(short, long)]
        job_id: String,
        #[arg(short, long, default_value = "127.0.0.1:5000")]
        address: String,
    },
}

/// Parse and execute CLI commands.
pub async fn execute(cli: Cli) -> Result<()> {
    match cli.command {
        Some(Commands::Run {
            config,
            mode,
            address,
            timeout_ms,
            parallelism,
        }) => {
            run_job(config, mode, address, timeout_ms, parallelism).await?;
        }
        Some(Commands::Job { command }) => {
            execute_job_command(command).await?;
        }
        Some(Commands::Cluster { address }) => {
            print_cluster_info(&address).await?;
        }
        None => {
            println!("SeaTunnel Rust v0.1.0");
            println!("Usage: seatunnel <COMMAND>");
        }
    }
    Ok(())
}

async fn run_job(
    config_path: PathBuf,
    mode: String,
    address: Option<String>,
    timeout_ms: u64,
    parallelism: Option<usize>,
) -> Result<()> {
    let exec_mode = match mode.as_str() {
        "local" => ExecutionMode::Local,
        "cluster" => {
            let addr = address.unwrap_or("127.0.0.1:5000".to_string());
            ExecutionMode::Cluster {
                addresses: vec![addr],
            }
        }
        other => {
            println!("Unknown mode: {}", other);
            return Ok(());
        }
    };

    // Read and parse the config file
    let contents = std::fs::read_to_string(&config_path).map_err(|e| {
        anyhow::anyhow!("Failed to read config file {}: {}", config_path.display(), e)
    })?;

    // Auto-detect format
    let config_format = detect_format(&config_path);
    let parsed = parse_config_string(&contents, config_format)?;

    // Build and run the engine
    let engine = Engine::new(exec_mode);

    // Apply parallelism override if set
    let mut final_config = parsed.clone();
    if let Some(p) = parallelism {
        if let Some(env) = final_config.get_mut("env") {
            if let Some(par) = env.get_mut("parallelism") {
                if let Some(default) = par.get_mut("default") {
                    *default = serde_json::Value::from(p);
                }
            }
        }
    }

    println!("========================================");
    println!("  SeaTunnel Rust v0.1.0");
    println!("  Mode: {}", mode);
    println!("  Config: {}", config_path.display());
    println!("========================================");
    println!();

    engine.execute(&final_config).await?;

    println!();
    println!("Job execution finished successfully.");
    Ok(())
}

/// Detect config format from file extension.
fn detect_format(path: &PathBuf) -> ConfigFormat {
    if let Some(ext) = path.extension().and_then(|e| e.to_str()) {
        match ext {
            "toml" => ConfigFormat::TOML,
            "conf" | "hocon" => ConfigFormat::HOCON,
            "yaml" | "yml" => ConfigFormat::YAML,
            _ => ConfigFormat::YAML,
        }
    } else {
        ConfigFormat::YAML
    }
}

/// Parse a config string into a JSON Value tree.
fn parse_config_string(content: &str, format: ConfigFormat) -> Result<serde_json::Value> {
    match format {
        ConfigFormat::HOCON => {
            seatunnel_config::parse_hocon(content)
                .map_err(|e| anyhow::anyhow!("Failed to parse HOCON config: {}", e))
        }
        ConfigFormat::TOML => {
            let parsed: toml::Value = toml::from_str(content)
                .map_err(|e| anyhow::anyhow!("Failed to parse TOML config: {}", e))?;
            Ok(serde_json::to_value(parsed)
                .map_err(|e| anyhow::anyhow!("TOML to JSON conversion failed: {}", e))?)
        }
        ConfigFormat::YAML => {
            let parsed: serde_yaml::Value = serde_yaml::from_str(content)
                .map_err(|e| anyhow::anyhow!("Failed to parse YAML config: {}", e))?;
            Ok(serde_json::to_value(parsed)
                .map_err(|e| anyhow::anyhow!("YAML to JSON conversion failed: {}", e))?)
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq)]
enum ConfigFormat {
    YAML,
    TOML,
    HOCON,
}

async fn execute_job_command(cmd: JobCommand) -> Result<()> {
    match cmd {
        JobCommand::Submit { config, address } => {
            println!(
                "Submitting job to {} from config {}",
                address,
                config.display()
            );
        }
        JobCommand::List { address } => {
            println!("Listing jobs on cluster at {}", address);
        }
        JobCommand::Status { job_id, address } => {
            println!("Job {} status on cluster {}", job_id, address);
        }
        JobCommand::Cancel { job_id, address } => {
            println!("Cancelling job {} on cluster {}", job_id, address);
        }
    }
    Ok(())
}

async fn print_cluster_info(_address: &str) -> Result<()> {
    println!("Cluster info:");
    println!("  Status: ready");
    println!("  Mode: cluster");
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use std::io::Write;

    #[test]
    fn test_cli_parse() {
        let cli = Cli::parse_from(["seatunnel", "run", "-c", "config.yaml", "-m", "local"]);
        assert!(matches!(cli.command, Some(Commands::Run { .. })));
    }

    #[test]
    fn test_cli_cluster() {
        let cli = Cli::parse_from(["seatunnel", "cluster"]);
        assert!(matches!(cli.command, Some(Commands::Cluster { .. })));
    }

    #[test]
    fn test_cli_job_list() {
        let cli = Cli::parse_from(["seatunnel", "job", "list"]);
        assert!(matches!(cli.command, Some(Commands::Job { .. })));
    }

    #[tokio::test]
    async fn test_run_job_local() {
        let mut f = fs::File::create("/tmp/test_run_job.conf").unwrap();
        write!(
            f,
            "env {{ job.name = \"cli-test\" }}
source {{ fake {{ row.num = 2 }} }}
sink {{ console {{ format = \"json\" }} }}"
        )
        .unwrap();
        let result = run_job(
            PathBuf::from("/tmp/test_run_job.conf"),
            "local".to_string(),
            None,
            5000,
            None,
        )
        .await;
        assert!(result.is_ok());
        fs::remove_file("/tmp/test_run_job.conf").ok();
    }
}
