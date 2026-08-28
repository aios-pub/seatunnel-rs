/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.
 */

//! SeaTunnel CLI: local execution, cluster job submission and management.

use anyhow::{Context, Result, bail};
use clap::{Parser, Subcommand};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use seatunnel_engine_client::EngineClient;

/// How often `--watch` polls job status.
const WATCH_POLL_MS: u64 = 1000;

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
    /// Run a job locally (in-process) or submit it to a cluster
    Run {
        /// Path to the job configuration file (.yaml/.yml/.conf/.toml)
        #[arg(short, long)]
        config: PathBuf,
        /// Execution mode: local or cluster
        #[arg(short, long, default_value = "local")]
        mode: String,
        /// Cluster master address (cluster mode)
        #[arg(short, long, default_value = "127.0.0.1:5800")]
        address: String,
        /// Parallelism override
        #[arg(long)]
        parallelism: Option<usize>,
        /// Stable job identity for local checkpointing / restart restore
        /// (defaults to env.job.id, else a fresh random id)
        #[arg(long)]
        job_id: Option<String>,
        /// Local checkpoint state directory (defaults to
        /// SEATUNNEL_STATE_DIR, else ./state)
        #[arg(long)]
        state_dir: Option<PathBuf>,
        /// Follow the submitted cluster job until it finishes
        #[arg(long, default_value_t = true)]
        watch: bool,
    },
    /// Manage jobs on a cluster
    Job {
        #[command(subcommand)]
        command: JobCommand,
    },
    /// Show cluster information
    Cluster {
        /// Cluster master address
        #[arg(short, long, default_value = "127.0.0.1:5800")]
        address: String,
    },
}

#[derive(Subcommand, Debug)]
pub enum JobCommand {
    /// Submit a job config file to the cluster
    Submit {
        #[arg(short, long)]
        config: PathBuf,
        #[arg(short, long, default_value = "127.0.0.1:5800")]
        address: String,
        /// Explicit job name (defaults to env.job.name or the file stem)
        #[arg(long)]
        name: Option<String>,
        /// Parallelism override
        #[arg(long)]
        parallelism: Option<usize>,
        /// Follow the job until it reaches a terminal state
        #[arg(long, default_value_t = false)]
        watch: bool,
    },
    /// List all jobs
    List {
        #[arg(short, long, default_value = "127.0.0.1:5800")]
        address: String,
    },
    /// Show detailed status of one job
    Status {
        #[arg(short, long)]
        job_id: String,
        #[arg(short, long, default_value = "127.0.0.1:5800")]
        address: String,
    },
    /// Cancel a running job
    Cancel {
        #[arg(short, long)]
        job_id: String,
        #[arg(short, long, default_value = "127.0.0.1:5800")]
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
            parallelism,
            job_id,
            state_dir,
            watch,
        }) => {
            if mode == "local" {
                run_local(config, parallelism, job_id, state_dir).await?;
            } else if mode == "cluster" {
                let job_id = submit_config(&config, &address, None, parallelism).await?;
                println!("Submitted job {}", job_id);
                if watch {
                    watch_job(&address, &job_id).await?;
                }
            } else {
                bail!("unknown mode '{}' (expected 'local' or 'cluster')", mode);
            }
        }
        Some(Commands::Job { command }) => match command {
            JobCommand::Submit {
                config,
                address,
                name,
                parallelism,
                watch,
            } => {
                let job_id = submit_config(&config, &address, name.as_deref(), parallelism).await?;
                println!("Submitted job {}", job_id);
                if watch {
                    watch_job(&address, &job_id).await?;
                }
            }
            JobCommand::List { address } => list_jobs(&address).await?,
            JobCommand::Status { job_id, address } => show_status(&address, &job_id).await?,
            JobCommand::Cancel { job_id, address } => cancel_job(&address, &job_id).await?,
        },
        Some(Commands::Cluster { address }) => print_cluster_info(&address).await?,
        None => {
            println!("SeaTunnel Rust v{}", env!("CARGO_PKG_VERSION"));
            println!("Usage: seatunnel <COMMAND>  (run | job | cluster)");
        }
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// Local execution
// ---------------------------------------------------------------------------

/// Run a job fully in-process using the same connector factory as workers.
async fn run_local(
    config_path: PathBuf,
    parallelism_override: Option<usize>,
    job_id_override: Option<String>,
    state_dir_override: Option<PathBuf>,
) -> Result<()> {
    use seatunnel_engine_core::connector_factory::{
        create_sink_pipeline, create_source, create_transforms, json_to_config_map,
    };
    use seatunnel_engine_core::local_checkpoint::{
        DEFAULT_CHECKPOINT_INTERVAL_MS, LocalCheckpointPlan, TaskRegistration,
    };
    use seatunnel_engine_core::task_group::{TaskContext, TaskGroup};

    let config = load_json_config(&config_path)?;
    let job_name = config
        .get("env")
        .and_then(|e| e.get("job"))
        .and_then(|j| j.get("name"))
        .and_then(|n| n.as_str())
        .map(str::to_string)
        .unwrap_or_else(|| "local-job".to_string());

    let parallelism = parallelism_override.unwrap_or_else(|| {
        config
            .get("env")
            .and_then(|e| e.get("parallelism"))
            .and_then(|p| p.as_u64())
            .unwrap_or(1)
            .max(1) as usize
    });

    // Stable job identity: without one, checkpoints cannot be addressed
    // across restarts, so checkpointing stays on but restore never matches.
    let job_id = job_id_override
        .or_else(|| {
            config
                .get("env")
                .and_then(|e| e.get("job"))
                .and_then(|j| j.get("id"))
                .and_then(|v| v.as_str())
                .map(str::to_string)
        })
        .unwrap_or_else(|| format!("local-{}", uuid_v4()));
    let state_root = state_dir_override
        .or_else(|| std::env::var("SEATUNNEL_STATE_DIR").ok().map(PathBuf::from))
        .unwrap_or_else(|| PathBuf::from("./state"));
    // env.checkpoint.interval (nested or dotted); 0 disables checkpointing.
    let checkpoint_interval_ms = config
        .get("env")
        .and_then(|env| {
            let nested = env.get("checkpoint").and_then(|c| match c {
                serde_json::Value::Number(n) => n.as_u64(),
                serde_json::Value::Object(m) => m.get("interval").and_then(|i| i.as_u64()),
                _ => None,
            });
            let flat = env.get("checkpoint.interval").and_then(|v| v.as_u64());
            nested.or(flat)
        })
        .unwrap_or(DEFAULT_CHECKPOINT_INTERVAL_MS);
    let checkpointing = checkpoint_interval_ms > 0;

    println!("========================================");
    println!("  SeaTunnel Rust v{} (local)", env!("CARGO_PKG_VERSION"));
    println!("  Config: {}", config_path.display());
    println!("  Job: {} × parallelism {}", job_name, parallelism);
    if checkpointing {
        println!(
            "  Checkpoint: every {}ms, state dir {}",
            checkpoint_interval_ms,
            state_root.display()
        );
    } else {
        println!("  Checkpoint: disabled (env.checkpoint.interval=0)");
    }
    println!("========================================");

    // Pipelines: explicit `pipelines` array (multi-source / fan-out) or the
    // legacy single source + sink sections (sink may be a list for
    // fan-out).
    struct LocalPipeline {
        name: String,
        parallelism: usize,
        source_plugin: String,
        source_cfg: serde_json::Value,
        sinks: Vec<seatunnel_engine_core::connector_factory::SinkDeclaration>,
        on_sink_failure: String,
    }
    let mut pipelines = Vec::new();
    if let Some(list) = config.get("pipelines").and_then(|v| v.as_array()) {
        for (idx, entry) in list.iter().enumerate() {
            let name = entry
                .get("name")
                .and_then(|v| v.as_str())
                .map(str::to_string)
                .unwrap_or_else(|| format!("p{}", idx));
            let pipe_parallelism = entry
                .get("parallelism")
                .and_then(|v| v.as_u64())
                .map(|v| (v as usize).max(1))
                .unwrap_or(parallelism);
            let source_section = entry
                .get("source")
                .ok_or_else(|| anyhow::anyhow!("pipeline '{}' has no source section", name))?;
            let (source_plugin, source_cfg) = first_section_value(source_section)
                .ok_or_else(|| anyhow::anyhow!("pipeline '{}' has an empty source", name))?;
            let sink_section = entry
                .get("sinks")
                .or_else(|| entry.get("sink"))
                .ok_or_else(|| anyhow::anyhow!("pipeline '{}' has no sinks section", name))?;
            let sinks =
                seatunnel_engine_core::connector_factory::parse_sink_declarations(sink_section)?;
            let on_sink_failure = entry
                .get("on-sink-failure")
                .and_then(|v| v.as_str())
                .or_else(|| {
                    config
                        .pointer("/env/on-sink-failure")
                        .and_then(|v| v.as_str())
                })
                .unwrap_or("fail")
                .to_string();
            pipelines.push(LocalPipeline {
                name,
                parallelism: pipe_parallelism,
                source_plugin,
                source_cfg,
                sinks,
                on_sink_failure,
            });
        }
    } else {
        let (source_plugin, source_cfg) = first_section(&config, "source")
            .ok_or_else(|| anyhow::anyhow!("config has no source section"))?;
        let sink_section = config
            .get("sink")
            .ok_or_else(|| anyhow::anyhow!("config has no sink section"))?;
        let sinks =
            seatunnel_engine_core::connector_factory::parse_sink_declarations(sink_section)?;
        pipelines.push(LocalPipeline {
            name: "pipeline".to_string(),
            parallelism,
            source_plugin,
            source_cfg: source_cfg.clone(),
            sinks,
            on_sink_failure: config
                .pointer("/env/on-sink-failure")
                .and_then(|v| v.as_str())
                .unwrap_or("fail")
                .to_string(),
        });
    }
    println!(
        "  Pipelines: {}",
        pipelines
            .iter()
            .map(|p| format!(
                "{}[{}: {} → {} sink(s)]",
                p.name,
                p.parallelism,
                p.source_plugin,
                p.sinks.len()
            ))
            .collect::<Vec<_>>()
            .join(", ")
    );

    // Checkpoint plan: register every task before spawning so the driver
    // knows the full task set, and load the restore point if one exists.
    let mut plan = if checkpointing {
        let mut plan = LocalCheckpointPlan::new(
            &state_root,
            &job_id,
            std::time::Duration::from_millis(checkpoint_interval_ms),
        );
        plan = plan.restore_from_latest()?;
        if let Some(envelope) = plan.restore_envelope() {
            println!(
                "  Restore: job {} from checkpoint {} ({})",
                job_id,
                envelope.checkpoint_id,
                if envelope.is_final {
                    "final"
                } else {
                    "interval"
                }
            );
        }
        plan
    } else {
        LocalCheckpointPlan::new(&state_root, &job_id, std::time::Duration::from_millis(1))
    };
    let restore_envelope = plan.restore_envelope().cloned();

    let cancel = std::sync::Arc::new(tokio_util::sync::CancellationToken::new());
    let shutdown = tokio_util::sync::CancellationToken::new();

    let transforms_cfg = transform_list(&config);

    let mut handles = Vec::new();
    for pipe in &pipelines {
        for subtask in 0..pipe.parallelism {
            let task_id = format!("{}-{}-local-{}", job_name, pipe.name, subtask);
            let gate = if checkpointing {
                Some(plan.register(TaskRegistration {
                    task_id: task_id.clone(),
                    pipeline: pipe.name.clone(),
                    subtask,
                    parallelism: pipe.parallelism,
                }))
            } else {
                None
            };

            // Reader: restore state comes from the envelope's task entry.
            let mut source_map = json_to_config_map(&pipe.source_cfg);
            source_map.insert("subtask.index".into(), subtask.to_string());
            source_map.insert("subtask.count".into(), pipe.parallelism.to_string());
            let reader_state = restore_envelope
                .as_ref()
                .and_then(|e| e.task_state(&pipe.name, subtask))
                .map(|t| t.reader_state.clone());
            let reader = create_source(
                &pipe.source_plugin,
                &source_map,
                pipe.parallelism,
                reader_state.as_deref(),
            )
            .with_context(|| format!("creating source '{}'", pipe.source_plugin))?;
            let transforms = create_transforms(&transforms_cfg).context("creating transforms")?;
            let policy =
                seatunnel_engine_core::fanout::SinkFailurePolicy::parse(&pipe.on_sink_failure);

            // Sink: namespace transactional ids per job/pipeline/subtask and
            // restore the writer state (last committed window). Only keys
            // the user did not set are injected.
            let writer_state = restore_envelope
                .as_ref()
                .and_then(|e| e.task_state(&pipe.name, subtask))
                .map(|t| t.writer_state.clone());
            let mut sinks = pipe.sinks.clone();
            for sink in &mut sinks {
                if let serde_json::Value::Object(map) = &mut sink.config {
                    map.entry("job.id")
                        .or_insert_with(|| serde_json::json!(job_id));
                    map.entry("pipeline.name")
                        .or_insert_with(|| serde_json::json!(pipe.name));
                    map.entry("subtask.index")
                        .or_insert_with(|| serde_json::json!(subtask));
                }
            }
            let sink_pipeline = create_sink_pipeline(&sinks, policy, writer_state.as_deref())
                .with_context(|| format!("creating sinks for pipeline '{}'", pipe.name))?;

            let mut context = TaskContext::new(
                task_id,
                job_id.clone(),
                pipe.name.clone(),
                subtask,
                pipe.parallelism,
            )
            .with_cancel_token(Arc::clone(&cancel));
            if let Some(gate) = gate {
                context = context.with_checkpoint_handle(gate);
            }
            let committer = sink_pipeline.committer;
            let writer = sink_pipeline.writer;
            handles.push(tokio::spawn(async move {
                let mut group = TaskGroup::new(context, reader, writer)
                    .with_transforms(transforms)
                    .with_committer(committer);
                group.run().await
            }));
        }
    }
    let _ = &pipelines;

    // Checkpoint driver + graceful shutdown on SIGINT/SIGTERM.
    let driver_join = if checkpointing {
        let driver = plan.build();
        let shutdown = shutdown.clone();
        Some(tokio::spawn(driver.run(shutdown, Arc::clone(&cancel))))
    } else {
        None
    };
    {
        let shutdown = shutdown.clone();
        let cancel = Arc::clone(&cancel);
        let has_driver = driver_join.is_some();
        tokio::spawn(async move {
            let sigterm = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate());
            let mut sigterm = match sigterm {
                Ok(s) => s,
                Err(_) => return,
            };
            tokio::select! {
                _ = tokio::signal::ctrl_c() => {}
                _ = sigterm.recv() => {}
            }
            println!("shutdown signal received, stopping gracefully...");
            // Without a checkpoint driver, cancel the tasks directly; with
            // one, the driver takes a final checkpoint first and then
            // cancels the tasks itself.
            if !has_driver {
                cancel.cancel();
            }
            shutdown.cancel();
        });
    }

    let mut failures = Vec::new();
    for handle in handles {
        let status = handle.await.context("task panicked")??;
        println!(
            "Task {} finished: {:?} records={}",
            status.task_id, status.state, status.processed_records
        );
        if let Some(err) = &status.error {
            failures.push(err.clone());
        }
    }
    if let Some(join) = driver_join {
        join.await
            .context("checkpoint driver panicked")?
            .context("checkpoint driver failed")?;
    }
    if !failures.is_empty() {
        bail!("local run failed: {}", failures.join("; "));
    }
    Ok(())
}

fn uuid_v4() -> String {
    // Lightweight unique suffix without pulling another dependency into scope.
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.subsec_nanos() as u64 ^ d.as_secs())
        .unwrap_or(0);
    format!("{:x}", nanos)
}

// ---------------------------------------------------------------------------
// Cluster interaction
// ---------------------------------------------------------------------------

/// Parse a config file into the canonical JSON tree.
pub fn load_json_config(path: &PathBuf) -> Result<serde_json::Value> {
    let contents = std::fs::read_to_string(path)
        .with_context(|| format!("reading config {}", path.display()))?;
    parse_config_string(&contents, detect_format(path))
}

fn detect_format(path: &Path) -> ConfigFormat {
    match path.extension().and_then(|e| e.to_str()) {
        Some("toml") => ConfigFormat::TOML,
        Some("conf") | Some("hocon") => ConfigFormat::HOCON,
        _ => ConfigFormat::YAML,
    }
}

#[derive(Debug, Clone, Copy)]
#[allow(clippy::upper_case_acronyms)]
enum ConfigFormat {
    YAML,
    TOML,
    HOCON,
}

fn parse_config_string(content: &str, format: ConfigFormat) -> Result<serde_json::Value> {
    match format {
        ConfigFormat::HOCON => {
            seatunnel_config::parse_hocon(content).map_err(|e| anyhow::anyhow!("HOCON: {}", e))
        }
        ConfigFormat::TOML => {
            let parsed: toml::Value =
                toml::from_str(content).map_err(|e| anyhow::anyhow!("TOML: {}", e))?;
            Ok(serde_json::to_value(parsed)?)
        }
        ConfigFormat::YAML => {
            let parsed: serde_yaml::Value =
                serde_yaml::from_str(content).map_err(|e| anyhow::anyhow!("YAML: {}", e))?;
            Ok(serde_json::to_value(parsed)?)
        }
    }
}

/// Extract `(plugin_name, config)` of the first entry of a section.
/// First `{Plugin: {...}}` block of a raw section value.
fn first_section_value(section: &serde_json::Value) -> Option<(String, serde_json::Value)> {
    match section {
        serde_json::Value::Object(map) => map.iter().next().map(|(k, v)| (k.clone(), v.clone())),
        serde_json::Value::Array(items) => items.first().and_then(|i| {
            i.as_object()
                .and_then(|m| m.iter().next())
                .map(|(k, v)| (k.clone(), v.clone()))
        }),
        _ => None,
    }
}

fn first_section<'a>(
    config: &'a serde_json::Value,
    section: &str,
) -> Option<(String, &'a serde_json::Value)> {
    let sec = config.get(section)?;
    match sec {
        serde_json::Value::Object(map) => map.iter().next().map(|(k, v)| (k.clone(), v)),
        serde_json::Value::Array(items) => items.first().and_then(|i| {
            i.as_object()
                .and_then(|m| m.iter().next())
                .map(|(k, v)| (k.clone(), v))
        }),
        _ => None,
    }
}

/// Normalize the transform section into an ordered array of configs.
fn transform_list(config: &serde_json::Value) -> Vec<serde_json::Value> {
    let Some(sec) = config.get("transform") else {
        return Vec::new();
    };
    match sec {
        serde_json::Value::Array(items) => items.clone(),
        serde_json::Value::Object(map) => map
            .iter()
            .map(|(name, cfg)| match cfg {
                serde_json::Value::Object(inner) => {
                    let mut full = inner.clone();
                    full.insert(
                        "plugin_name".into(),
                        serde_json::Value::String(name.clone()),
                    );
                    serde_json::Value::Object(full)
                }
                other => serde_json::json!({ "plugin_name": name, "config": other }),
            })
            .collect(),
        _ => Vec::new(),
    }
}

/// Submit a config file to the master; returns the assigned job id.
async fn submit_config(
    config_path: &PathBuf,
    address: &str,
    name_override: Option<&str>,
    parallelism: Option<usize>,
) -> Result<String> {
    let config = load_json_config(config_path)?;
    let job_name = name_override
        .map(str::to_string)
        .or_else(|| {
            config
                .get("env")
                .and_then(|e| e.get("job"))
                .and_then(|j| j.get("name"))
                .and_then(|n| n.as_str())
                .map(str::to_string)
        })
        .unwrap_or_else(|| {
            config_path
                .file_stem()
                .map(|s| s.to_string_lossy().to_string())
                .unwrap_or_else(|| "job".to_string())
        });

    let job_id = format!("job-{}", uuid::Uuid::new_v4());
    let body = serde_json::to_vec(&config)?;

    let client = EngineClient::new(address);
    let resp = client
        .submit_job(&job_id, &job_name, body, parallelism.unwrap_or(0) as i32)
        .await
        .map_err(|e| anyhow::anyhow!("submit failed: {}", e))?;

    if !resp.success {
        anyhow::bail!("master rejected job: {}", resp.message);
    }
    Ok(resp.job_id)
}

fn state_name(code: i32) -> &'static str {
    match code {
        2 => "SCHEDULED",
        3 => "RUNNING",
        4 => "COMPLETED",
        5 => "FAILED",
        6 => "CANCELLED",
        _ => "CREATED",
    }
}

async fn watch_job(address: &str, job_id: &str) -> Result<()> {
    let client = EngineClient::new(address);
    println!("Watching job {} on {} (Ctrl-C to detach)…", job_id, address);
    loop {
        tokio::time::sleep(Duration::from_millis(WATCH_POLL_MS)).await;
        match client.get_job_status(job_id).await {
            Ok(status) => {
                let records: i64 = status.tasks.iter().map(|t| t.processed_records).sum();
                println!(
                    "[{}] {} records={} tasks={}",
                    state_name(status.state),
                    status.job_name,
                    records,
                    status.tasks.len()
                );
                if matches!(status.state, 4..=6) {
                    if !status.error_message.is_empty() {
                        bail!("job failed: {}", status.error_message);
                    }
                    return Ok(());
                }
            }
            Err(e) => return Err(anyhow::anyhow!("status query failed: {}", e)),
        }
    }
}

async fn list_jobs(address: &str) -> Result<()> {
    let client = EngineClient::new(address);
    let jobs = client
        .list_jobs()
        .await
        .map_err(|e| anyhow::anyhow!("{}", e))?;
    println!("{:<40} {:<28} {:<12} STARTED", "JOB_ID", "NAME", "STATE");
    for j in jobs.jobs {
        println!(
            "{:<40} {:<28} {:<12} {}",
            j.job_id,
            j.job_name,
            state_name(j.state),
            j.start_time
        );
    }
    Ok(())
}

async fn show_status(address: &str, job_id: &str) -> Result<()> {
    let client = EngineClient::new(address);
    let s = client
        .get_job_status(job_id)
        .await
        .map_err(|e| anyhow::anyhow!("{}", e))?;
    println!("Job       : {}", s.job_id);
    println!("Name      : {}", s.job_name);
    println!("State     : {}", state_name(s.state));
    if !s.error_message.is_empty() {
        println!("Error     : {}", s.error_message);
    }
    println!("Tasks:");
    for t in s.tasks {
        println!(
            "  {:<44} {:<10} records={}",
            t.task_id,
            match t.state {
                2 => "RUNNING",
                3 => "COMPLETED",
                4 => "FAILED",
                5 => "CANCELLED",
                _ => "CREATED",
            },
            t.processed_records
        );
    }
    Ok(())
}

async fn cancel_job(address: &str, job_id: &str) -> Result<()> {
    let client = EngineClient::new(address);
    client
        .cancel_job(job_id)
        .await
        .map_err(|e| anyhow::anyhow!("{}", e))?;
    println!("Cancel request sent for job {}", job_id);
    Ok(())
}

async fn print_cluster_info(address: &str) -> Result<()> {
    let client = EngineClient::new(address);
    let info = client
        .get_cluster_info()
        .await
        .map_err(|e| anyhow::anyhow!("{}", e))?;
    println!(
        "Cluster leader   : {} (term {}, node role: {})",
        if info.leader_id.is_empty() {
            "-"
        } else {
            &info.leader_id
        },
        info.term,
        if info.role.is_empty() {
            "-"
        } else {
            &info.role
        }
    );
    println!("Workers          : {}", info.available_workers);
    println!(
        "Total tasks      : {} (running {})",
        info.total_tasks, info.running_tasks
    );
    for w in info.workers {
        println!(
            "  ● {} @ {} (last hb {}, slots {}, running {})",
            w.worker_id,
            w.address,
            w.last_heartbeat,
            if w.slots == 0 {
                "-".to_string()
            } else {
                w.slots.to_string()
            },
            w.running_tasks
        );
    }
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
    fn test_cli_job_list() {
        let cli = Cli::parse_from(["seatunnel", "job", "list"]);
        assert!(matches!(cli.command, Some(Commands::Job { .. })));
    }

    #[test]
    fn test_parse_yaml_config_tree() {
        let yaml = r#"
env:
  job.name: cdc-demo
  parallelism: 1
source:
  MySQL-CDC:
    hostname: localhost
    port: 3306
sink:
  Kafka:
    topic: out
"#;
        let v = parse_config_string(yaml, ConfigFormat::YAML).unwrap();
        assert_eq!(first_section(&v, "source").unwrap().0, "MySQL-CDC");
        assert_eq!(first_section(&v, "sink").unwrap().0, "Kafka");
        assert!(transform_list(&v).is_empty());
    }

    #[tokio::test]
    async fn test_run_local_fake_pipeline() {
        let mut f = fs::File::create("/tmp/st-cli-local.yaml").unwrap();
        write!(
            f,
            "env:\n  job.name: cli-test\nsource:\n  Fake:\n    row.num: 2\nsink:\n  Console:\n"
        )
        .unwrap();
        let result = run_local(PathBuf::from("/tmp/st-cli-local.yaml"), None, None, None).await;
        assert!(result.is_ok(), "{result:?}");
        fs::remove_file("/tmp/st-cli-local.yaml").ok();
    }

    #[tokio::test]
    async fn test_submit_rejects_when_master_unreachable() {
        let result = submit_config(
            &PathBuf::from("/tmp/nonexistent.yaml"),
            "127.0.0.1:1",
            None,
            None,
        )
        .await;
        assert!(result.is_err());
    }
}
