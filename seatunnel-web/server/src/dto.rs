/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.
 */

//! REST API data transfer objects shared with the embedded frontend.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

/// Job summary shown in list views.
#[derive(Debug, Clone, Serialize)]
pub struct JobSummaryDto {
    pub job_id: String,
    pub job_name: String,
    /// Human-readable job state (CREATED/SCHEDULED/RUNNING/...).
    pub state: String,
    pub start_time_ms: i64,
    pub end_time_ms: i64,
}

/// Sink-side delivery metrics of one task (windowed, not lifetime).
#[derive(Debug, Clone, Serialize)]
pub struct SinkMetricsDto {
    /// Sliding window length the counts cover, seconds.
    pub window_secs: u64,
    /// Messages enqueued to the transport within the window.
    pub sent: u64,
    /// Delivery reports acknowledged OK within the window.
    pub delivered: u64,
    /// Delivery reports failed within the window.
    pub failed: u64,
    /// Messages currently in flight (enqueued, report pending).
    pub in_flight: u64,
    /// EMA of enqueue→report latency, millis.
    pub latency_ema_ms: f64,
    /// Max enqueue→report latency within the window, millis.
    pub latency_max_ms: u64,
    /// Detailed last delivery error (empty when none).
    pub last_error: String,
    /// Epoch-ms of `last_error` (0 = none).
    pub last_error_at: i64,
}

/// Per-task execution status inside a job.
#[derive(Debug, Clone, Serialize)]
pub struct TaskStatusDto {
    pub task_id: String,
    pub stage_id: String,
    pub state: String,
    /// Worker currently (or last known) executing this task.
    #[serde(default)]
    pub worker_id: String,
    pub processed_records: i64,
    /// Epoch-ms of the most recent record (0 = none yet).
    pub last_record_ms: i64,
    /// Records per second, derived from consecutive samples (0 on the
    /// first observation after startup).
    pub records_per_sec: f64,
    /// Milliseconds since the last processed record — the liveness signal
    /// for streaming tasks (-1 when no record was processed yet).
    pub idle_ms: i64,
    /// Sink delivery metrics (absent when the sink reports nothing).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sink_metrics: Option<SinkMetricsDto>,
}

/// Full job status for the detail view.
#[derive(Debug, Clone, Serialize)]
pub struct JobStatusDto {
    pub job_id: String,
    pub job_name: String,
    pub state: String,
    pub start_time_ms: i64,
    pub end_time_ms: i64,
    pub error_message: String,
    pub checkpoint_interval_ms: i64,
    pub checkpoints_completed: i64,
    pub tasks: Vec<TaskStatusDto>,
    /// The job config exactly as submitted (JSON) — the edit basis for
    /// update-and-restart.
    #[serde(default)]
    pub job_config: String,
}

/// Request body for `POST /api/v1/jobs/{job_id}/update`.
#[derive(Debug, Clone, Deserialize)]
pub struct UpdateJobDto {
    /// Edited job config (JSON text, as returned by job_config).
    pub config_text: String,
    /// Config format: "json" (default; the edit basis is JSON).
    pub format: Option<String>,
    /// Optional display name override.
    pub job_name: Option<String>,
    /// Optional parallelism override; 0 = keep config value.
    pub parallelism: Option<i32>,
    /// Max seconds to wait for the old job to cancel before aborting.
    #[serde(default = "default_cancel_timeout")]
    pub cancel_timeout_secs: u64,
}

fn default_cancel_timeout() -> u64 {
    60
}

/// Result of an update-and-restart.
#[derive(Debug, Clone, Serialize)]
pub struct UpdateResultDto {
    pub job_id: String,
    pub cancelled: bool,
    pub cancel_wait_ms: u64,
    pub message: String,
}

/// Request body for `POST /api/v1/jobs`.
#[derive(Debug, Clone, Deserialize)]
pub struct SubmitJobDto {
    /// Raw job config text (YAML by default).
    pub config_text: String,
    /// Config format: "yaml" (default), "toml" or "hocon".
    pub format: Option<String>,
    /// Optional display name; defaults to `job-<uuid prefix>`.
    pub job_name: Option<String>,
    /// Optional parallelism override; 0 = use config value.
    pub parallelism: Option<i32>,
}

/// Response for `POST /api/v1/jobs`.
#[derive(Debug, Clone, Serialize)]
pub struct SubmitResultDto {
    pub job_id: String,
    pub message: String,
}

/// One worker entry of the cluster, including its measured admission
/// state (dynamic task admission: pressure signals, not slot counts).
#[derive(Debug, Clone, Serialize)]
pub struct WorkerDto {
    pub worker_id: String,
    pub address: String,
    pub last_heartbeat_ms: i64,
    pub running_tasks: i32,
    /// Measured pressure 0..1000 (per-mille).
    pub load_score_permille: u32,
    /// Event-loop lag EMA (ms) — runtime saturation signal.
    pub lag_ms: u32,
    /// RSS over usable memory, 0..1000 (per-mille).
    pub mem_permille: u32,
    /// False while the worker is over a pressure watermark: it receives
    /// no new tasks and its pending tasks may be stolen by peers.
    pub can_accept: bool,
}

/// Cluster snapshot for the cluster view.
#[derive(Debug, Clone, Serialize)]
pub struct ClusterInfoDto {
    pub leader_id: String,
    /// Fencing term of the responding master (raft leader term).
    pub leader_term: u64,
    /// Role of the responding node: master | worker | hybrid.
    pub leader_role: String,
    pub available_workers: i32,
    pub total_tasks: i32,
    pub running_tasks: i32,
    pub workers: Vec<WorkerDto>,
}

/// One retained checkpoint of a task.
#[derive(Debug, Clone, Serialize)]
pub struct CheckpointEntryDto {
    pub checkpoint_id: i64,
    pub size_bytes: i64,
}

/// Checkpoint history of one task.
#[derive(Debug, Clone, Serialize)]
pub struct TaskCheckpointDto {
    pub task_id: String,
    pub entries: Vec<CheckpointEntryDto>,
}

/// Log lines of one task.
#[derive(Debug, Clone, Serialize)]
pub struct TaskLogsDto {
    pub task_id: String,
    pub lines: Vec<String>,
}

/// Per-task logs of a job for the detail view.
#[derive(Debug, Clone, Serialize)]
pub struct JobLogsDto {
    pub job_id: String,
    pub tasks: Vec<TaskLogsDto>,
}

/// Checkpoint history of a job for the detail view.
#[derive(Debug, Clone, Serialize)]
pub struct CheckpointHistoryDto {
    pub job_id: String,
    pub checkpoint_interval_ms: i64,
    pub checkpoints_completed: i64,
    pub tasks: Vec<TaskCheckpointDto>,
}

/// Dashboard overview aggregating job counts and cluster info.
#[derive(Debug, Clone, Serialize)]
pub struct OverviewDto {
    pub jobs_total: i64,
    pub jobs_running: i64,
    pub jobs_pending: i64,
    pub jobs_completed: i64,
    pub jobs_failed: i64,
    pub jobs_cancelled: i64,
    /// Extra states (e.g. SCHEDULED) keyed by name, for display.
    pub jobs_by_state: BTreeMap<String, i64>,
    pub cluster: ClusterInfoDto,
}

/// Health report combining web liveness and master reachability.
#[derive(Debug, Clone, Serialize)]
pub struct HealthDto {
    pub status: &'static str,
    pub master: String,
    #[serde(skip_serializing_if = "String::is_empty")]
    pub error: String,
}

/// Unified error body for all failing REST calls.
#[derive(Debug, Clone, Serialize)]
pub struct ErrorDto {
    pub error: String,
}
