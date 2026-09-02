// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements.

//! REST client for the seatunnel-web backend (`/api/v1`).

use leptos::prelude::*;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};

const BASE: &str = "/api/v1";

#[derive(Clone, Debug, Deserialize)]
pub struct Health {
    pub status: String,
    pub master: String,
    #[serde(default)]
    pub error: String,
}

#[derive(Clone, Debug, Deserialize)]
pub struct Overview {
    pub jobs_total: i64,
    pub jobs_running: i64,
    pub jobs_pending: i64,
    pub jobs_completed: i64,
    pub jobs_failed: i64,
    pub jobs_cancelled: i64,
    pub jobs_by_state: std::collections::BTreeMap<String, i64>,
    pub cluster: ClusterInfo,
}

#[derive(Clone, Debug, Deserialize)]
pub struct ClusterInfo {
    pub leader_id: String,
    #[serde(default)]
    pub leader_term: u64,
    #[serde(default)]
    pub leader_role: String,
    pub available_workers: i32,
    pub total_tasks: i32,
    pub running_tasks: i32,
    /// Known raft member addresses (master/hybrid nodes).
    #[serde(default)]
    pub raft_members: Vec<String>,
    #[serde(default)]
    pub workers: Vec<Worker>,
}

#[derive(Clone, Debug, Deserialize)]
pub struct Worker {
    pub worker_id: String,
    pub address: String,
    pub last_heartbeat_ms: i64,
    pub running_tasks: i32,
    /// Measured pressure 0..1000 (per-mille).
    #[serde(default)]
    pub load_score_permille: u32,
    /// Event-loop lag EMA (ms).
    #[serde(default)]
    pub lag_ms: u32,
    /// RSS over usable memory, 0..1000 (per-mille).
    #[serde(default)]
    pub mem_permille: u32,
    /// Host CPU usage, 0..1000 (per-mille).
    #[serde(default)]
    pub cpu_permille: u32,
    /// Task ids currently owned by this worker (Running/Deploying).
    #[serde(default)]
    pub task_ids: Vec<String>,
    /// False while over a pressure watermark (no new tasks; pending tasks
    /// may be stolen by healthy peers).
    #[serde(default = "default_true")]
    pub can_accept: bool,
}

fn default_true() -> bool {
    true
}

#[derive(Clone, Debug, Deserialize)]
pub struct JobSummary {
    pub job_id: String,
    pub job_name: String,
    pub state: String,
    pub start_time_ms: i64,
    pub end_time_ms: i64,
}

#[derive(Clone, Debug, Deserialize)]
pub struct JobStatus {
    pub job_id: String,
    pub job_name: String,
    pub state: String,
    pub start_time_ms: i64,
    pub end_time_ms: i64,
    pub error_message: String,
    pub checkpoint_interval_ms: i64,
    pub checkpoints_completed: i64,
    /// The job config exactly as submitted (JSON) — edit basis for
    /// update-and-restart.
    #[serde(default)]
    pub job_config: String,
    /// Requested task parallelism (0 = unknown).
    #[serde(default)]
    pub parallelism: i32,
    #[serde(default)]
    pub tasks: Vec<TaskStatus>,
}

#[derive(Clone, Debug, Deserialize)]
pub struct TaskStatus {
    pub task_id: String,
    pub stage_id: String,
    pub state: String,
    #[serde(default)]
    pub worker_id: String,
    pub processed_records: i64,
    /// Epoch-ms of the most recent record (0 = none yet).
    #[serde(default)]
    pub last_record_ms: i64,
    /// Records per second from consecutive samples.
    #[serde(default)]
    pub records_per_sec: f64,
    /// Ms since the last record (-1 = none yet).
    #[serde(default)]
    pub idle_ms: i64,
    /// Sink delivery metrics (absent when the sink reports nothing).
    #[serde(default)]
    pub sink_metrics: Option<SinkMetrics>,
    /// Last failure detail reported by the task (empty when healthy).
    #[serde(default)]
    pub error: String,
}

/// Sink-side delivery metrics of one task (windowed, not lifetime).
#[derive(Debug, Clone, Default, serde::Deserialize, PartialEq)]
pub struct SinkMetrics {
    #[serde(default)]
    pub window_secs: u64,
    #[serde(default)]
    pub sent: u64,
    #[serde(default)]
    pub delivered: u64,
    #[serde(default)]
    pub failed: u64,
    #[serde(default)]
    pub in_flight: u64,
    #[serde(default)]
    pub latency_ema_ms: f64,
    #[serde(default)]
    pub latency_max_ms: u64,
    #[serde(default)]
    pub last_error: String,
    #[serde(default)]
    pub last_error_at: i64,
}

#[derive(Clone, Debug, Deserialize)]
pub struct CheckpointHistory {
    pub checkpoint_interval_ms: i64,
    pub checkpoints_completed: i64,
    #[serde(default)]
    pub tasks: Vec<TaskCheckpoint>,
}

#[derive(Clone, Debug, Deserialize)]
pub struct TaskCheckpoint {
    pub task_id: String,
    #[serde(default)]
    pub entries: Vec<CheckpointEntry>,
}

#[derive(Clone, Debug, Deserialize)]
pub struct CheckpointEntry {
    pub checkpoint_id: i64,
    pub size_bytes: i64,
}

/// Log lines of one task.
#[derive(Clone, Debug, Deserialize)]
pub struct TaskLogs {
    pub task_id: String,
    #[serde(default)]
    pub lines: Vec<String>,
}

/// Per-task logs of a job.
#[derive(Clone, Debug, Deserialize)]
pub struct JobLogs {
    pub job_id: String,
    #[serde(default)]
    pub tasks: Vec<TaskLogs>,
}

#[derive(Clone, Debug, Serialize)]
pub struct SubmitJobRequest {
    pub config_text: String,
    pub format: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub job_name: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub parallelism: Option<i32>,
}

#[derive(Clone, Debug, Deserialize)]
pub struct SubmitResult {
    pub job_id: String,
    pub message: String,
}

/// Identity returned by the login and whoami endpoints.
#[derive(Clone, Debug, Deserialize)]
pub struct Whoami {
    pub username: String,
}

async fn get<T: DeserializeOwned>(path: &str) -> Result<T, String> {
    let resp = gloo_net::http::Request::get(&format!("{}{}", BASE, path))
        .send()
        .await
        .map_err(|e| format!("request failed: {}", e))?;
    read_json(resp).await
}

async fn read_json<T: DeserializeOwned>(resp: gloo_net::http::Response) -> Result<T, String> {
    if !resp.ok() {
        let status = resp.status();
        // An expired session reloads the app, which bounces back to the
        // login screen via the whoami probe.
        if status == 401 {
            let _ = window().location().assign("/login");
            return Err("session expired".to_string());
        }
        let text = resp.text().await.unwrap_or_default();
        return Err(extract_error(&text).unwrap_or_else(|| format!("HTTP {}", status)));
    }
    resp.json::<T>()
        .await
        .map_err(|e| format!("invalid response body: {}", e))
}

/// Pull the `{"error": "..."}` field out of a backend error body if present.
fn extract_error(text: &str) -> Option<String> {
    let value: serde_json::Value = serde_json::from_str(text).ok()?;
    value
        .get("error")?
        .as_str()
        .map(|message| format!("HTTP error: {}", message))
}

pub async fn health() -> Result<Health, String> {
    get("/health").await
}

/// Check the current session; 401 maps to `Err` (logged out). Implemented
/// separately from `read_json` so the probe never triggers the global
/// redirect-to-login logic — that would reload the page in a loop while
/// the user is signed out.
pub async fn whoami() -> Result<Whoami, String> {
    let resp = gloo_net::http::Request::get(&format!("{}/whoami", BASE))
        .send()
        .await
        .map_err(|e| format!("request failed: {}", e))?;
    if resp.status() == 401 {
        return Err("logged out".to_string());
    }
    resp.json::<Whoami>()
        .await
        .map_err(|e| format!("invalid response body: {}", e))
}

/// Submit credentials; success returns the logged-in identity.
/// Implemented separately from `read_json` so a 401 here does not trigger
/// the global redirect-to-login logic.
pub async fn login(username: String, password: String) -> Result<Whoami, String> {
    let resp = gloo_net::http::Request::post(&format!("{}/login", BASE))
        .json(&serde_json::json!({ "username": username, "password": password }))
        .map_err(|e| format!("request failed: {}", e))?
        .send()
        .await
        .map_err(|e| format!("request failed: {}", e))?;
    if !resp.ok() {
        return Err("invalid username or password".to_string());
    }
    resp.json::<Whoami>()
        .await
        .map_err(|e| format!("invalid response body: {}", e))
}

/// Clear the session cookie; errors are ignored (best effort).
pub async fn logout() {
    let _ = gloo_net::http::Request::post(&format!("{}/logout", BASE))
        .send()
        .await;
}

pub async fn overview() -> Result<Overview, String> {
    get("/overview").await
}

pub async fn cluster() -> Result<ClusterInfo, String> {
    get("/cluster").await
}

pub async fn jobs() -> Result<Vec<JobSummary>, String> {
    get("/jobs").await
}

pub async fn job_status(job_id: &str) -> Result<JobStatus, String> {
    get(&format!("/jobs/{}", job_id)).await
}

pub async fn job_checkpoints(job_id: &str) -> Result<CheckpointHistory, String> {
    get(&format!("/jobs/{}/checkpoints", job_id)).await
}

pub async fn job_logs(job_id: &str) -> Result<JobLogs, String> {
    get(&format!("/jobs/{}/logs", job_id)).await
}

pub async fn submit_job(request: SubmitJobRequest) -> Result<SubmitResult, String> {
    let resp = gloo_net::http::Request::post(&format!("{}/jobs", BASE))
        .json(&request)
        .map_err(|e| format!("request failed: {}", e))?
        .send()
        .await
        .map_err(|e| format!("request failed: {}", e))?;
    read_json(resp).await
}

#[derive(Clone, Debug, serde::Serialize)]
pub struct UpdateJobRequest {
    pub config_text: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub job_name: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub parallelism: Option<i32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cancel_timeout_secs: Option<u64>,
}

#[derive(Clone, Debug, Deserialize)]
pub struct UpdateResult {
    pub job_id: String,
    pub cancelled: bool,
    pub cancel_wait_ms: u64,
    pub message: String,
}

/// Edit-and-restart: cancel (exit checkpoint) → resubmit the same id.
/// Long-running (up to the cancel timeout).
pub async fn update_job(job_id: &str, request: UpdateJobRequest) -> Result<UpdateResult, String> {
    let resp = gloo_net::http::Request::post(&format!("{}/jobs/{}/update", BASE, job_id))
        .json(&request)
        .map_err(|e| format!("request failed: {}", e))?
        .send()
        .await
        .map_err(|e| format!("request failed: {}", e))?;
    read_json(resp).await
}

pub async fn cancel_job(job_id: &str) -> Result<(), String> {
    let resp = gloo_net::http::Request::post(&format!("{}/jobs/{}/cancel", BASE, job_id))
        .send()
        .await
        .map_err(|e| format!("request failed: {}", e))?;
    if resp.ok() {
        Ok(())
    } else {
        let status = resp.status();
        let text = resp.text().await.unwrap_or_default();
        Err(extract_error(&text).unwrap_or_else(|| format!("HTTP {}", status)))
    }
}

/// Restart a historical job with its retained config (same id, checkpoint
/// restore). Long-running (up to the server-side cancel timeout).
pub async fn restart_job(job_id: &str) -> Result<SubmitResult, String> {
    let resp = gloo_net::http::Request::post(&format!("{}/jobs/{}/restart", BASE, job_id))
        .send()
        .await
        .map_err(|e| format!("request failed: {}", e))?;
    read_json(resp).await
}

/// Delete a TERMINAL job from history (state + checkpoint metadata).
pub async fn delete_job(job_id: &str) -> Result<(), String> {
    let resp = gloo_net::http::Request::delete(&format!("{}/jobs/{}", BASE, job_id))
        .send()
        .await
        .map_err(|e| format!("request failed: {}", e))?;
    if resp.ok() {
        Ok(())
    } else {
        let status = resp.status();
        let text = resp.text().await.unwrap_or_default();
        Err(extract_error(&text).unwrap_or_else(|| format!("HTTP {}", status)))
    }
}
