/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.
 */

//! Job management handlers.

use axum::Json;
use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use uuid::Uuid;

use crate::api::error_response;
use crate::dto::{CheckpointHistoryDto, SubmitJobDto, UpdateJobDto};
use crate::{AppState, EngineError};

/// `GET /api/v1/jobs` — all jobs, newest first.
pub async fn list_jobs(State(state): State<AppState>) -> Response {
    match state.engine.list_jobs().await {
        Ok(mut jobs) => {
            jobs.sort_by(|a, b| b.start_time_ms.cmp(&a.start_time_ms));
            Json(jobs).into_response()
        }
        Err(e) => error_response(&e),
    }
}

/// `GET /api/v1/jobs/{job_id}` — full job status including tasks with
/// derived throughput (records/s) and idle time (since the last record).
pub async fn job_detail(State(state): State<AppState>, Path(job_id): Path<String>) -> Response {
    let mut status = match state.engine.job_status(&job_id).await {
        Ok(status) => status,
        Err(e) => return error_response(&e),
    };

    // Derive per-task rate/idle from consecutive reads.
    let now_ms = now_ms();
    let mut samples = state.task_samples.lock().unwrap();
    for task in &mut status.tasks {
        let sample_key = format!("{}:{}", status.job_id, task.task_id);
        let rate = match samples.get(&sample_key) {
            Some((prev_records, prev_at))
                if now_ms > *prev_at && task.processed_records >= *prev_records =>
            {
                let dt = (now_ms - prev_at) as f64 / 1000.0;
                ((task.processed_records - prev_records) as f64 / dt).max(0.0)
            }
            _ => 0.0,
        };
        task.records_per_sec = (rate * 10.0).round() / 10.0;
        task.idle_ms = if task.last_record_ms > 0 {
            (now_ms - task.last_record_ms).max(0)
        } else {
            -1
        };
        samples.insert(sample_key, (task.processed_records, now_ms));
    }
    // Drop samples of tasks that disappeared (job finished/evicted).
    let prefix = format!("{}:", status.job_id);
    samples.retain(|key, _| key.starts_with(&prefix));

    Json(status).into_response()
}

/// `GET /api/v1/jobs/{job_id}/logs` — per-task log lines (lifecycle
/// events + sampled data rows).
pub async fn job_logs(State(state): State<AppState>, Path(job_id): Path<String>) -> Response {
    match state.engine.job_logs(&job_id).await {
        Ok(logs) => Json(logs).into_response(),
        Err(e) => error_response(&e),
    }
}

fn now_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

/// `POST /api/v1/jobs` — validate and submit a job config to the master.
pub async fn submit_job(
    State(state): State<AppState>,
    Json(request): Json<SubmitJobDto>,
) -> Response {
    let format = match parse_format(request.format.as_deref()) {
        Ok(f) => f,
        Err(e) => return bad_request(e),
    };

    // Parse and validate the config text before it reaches the master.
    let parsed = match seatunnel_config::parse_config_file(&request.config_text, format) {
        Ok(parsed) => parsed,
        Err(e) => return bad_request(format!("config parse error: {}", e)),
    };
    if parsed.sources.is_empty() {
        return bad_request("config has no source section".to_string());
    }
    if parsed.sinks.is_empty() {
        return bad_request("config has no sink section".to_string());
    }

    // Rebuild the canonical JSON document the master's compiler expects.
    let mut doc = serde_json::Map::new();
    if let Some(env) = parsed.env {
        doc.insert("env".to_string(), env);
    }
    doc.insert(
        "source".to_string(),
        serde_json::Value::Array(parsed.sources),
    );
    doc.insert(
        "transform".to_string(),
        serde_json::Value::Array(parsed.transforms),
    );
    doc.insert("sink".to_string(), serde_json::Value::Array(parsed.sinks));
    let config_bytes = match serde_json::to_vec(&serde_json::Value::Object(doc)) {
        Ok(bytes) => bytes,
        Err(e) => return bad_request(format!("config serialization error: {}", e)),
    };

    let job_id = format!("job-{}", Uuid::new_v4());
    match state.engine.submit_job(request, job_id, config_bytes).await {
        Ok(result) => (StatusCode::OK, Json(result)).into_response(),
        Err(e) => error_response(&e),
    }
}

/// `POST /api/v1/jobs/{job_id}/cancel` — cancel a running job.
/// `POST /api/v1/jobs/{job_id}/update` — edit-and-restart with the same
/// job id (checkpoint restore). Long-running by design (≤ cancel timeout).
pub async fn update_job(
    State(state): State<AppState>,
    Path(job_id): Path<String>,
    Json(body): Json<UpdateJobDto>,
) -> Response {
    // The edit basis is the JSON returned by job detail; accept only JSON
    // (other formats are submit-time concerns).
    let format = body.format.as_deref().unwrap_or("json");
    if format != "json" {
        return error_response(&EngineError::Invalid(
            "update accepts JSON config text (as returned by the job detail's job_config)"
                .to_string(),
        ));
    }
    let config_bytes = match body.config_text.trim() {
        "" => {
            return error_response(&EngineError::Invalid(
                "config_text must not be empty".to_string(),
            ));
        }
        text => serde_json::from_str::<serde_json::Value>(text)
            .map_err(|e| EngineError::Invalid(format!("invalid JSON config: {}", e)))
            .and_then(|v| serde_json::to_vec(&v).map_err(|e| EngineError::Invalid(e.to_string())))
            .and_then(|b| Ok(b)),
    };
    let config_bytes = match config_bytes {
        Ok(b) => b,
        Err(e) => return error_response(&e),
    };
    // Default the name to the job id stem when not provided.
    let job_name = body.job_name.clone().unwrap_or_else(|| job_id.clone());
    match state
        .engine
        .update_job(
            &job_id,
            &job_name,
            config_bytes,
            body.parallelism.unwrap_or(0),
            body.cancel_timeout_secs,
        )
        .await
    {
        Ok(result) => Json(result).into_response(),
        Err(e) => error_response(&e),
    }
}

pub async fn cancel_job(State(state): State<AppState>, Path(job_id): Path<String>) -> Response {
    match state.engine.cancel_job(&job_id).await {
        Ok(()) => (
            StatusCode::OK,
            Json(serde_json::json!({ "cancelled": job_id })),
        )
            .into_response(),
        Err(e) => error_response(&e),
    }
}

/// `POST /api/v1/jobs/{job_id}/restart` — restart a historical job with
/// its retained config (same id, checkpoint restore). Long-running by
/// design (≤ the server-side cancel timeout).
pub async fn restart_job(State(state): State<AppState>, Path(job_id): Path<String>) -> Response {
    match state.engine.restart_job(&job_id).await {
        Ok(result) => (StatusCode::OK, Json(result)).into_response(),
        Err(e) => error_response(&e),
    }
}

/// `DELETE /api/v1/jobs/{job_id}` — remove a TERMINAL job from history.
/// The engine rejects deleting a non-terminal job (mapped to 400).
pub async fn delete_job(State(state): State<AppState>, Path(job_id): Path<String>) -> Response {
    match state.engine.delete_job(&job_id).await {
        Ok(()) => (
            StatusCode::OK,
            Json(serde_json::json!({ "deleted": job_id })),
        )
            .into_response(),
        Err(e) => error_response(&e),
    }
}

/// `GET /api/v1/jobs/{job_id}/checkpoints` — checkpoint history.
pub async fn job_checkpoints(
    State(state): State<AppState>,
    Path(job_id): Path<String>,
) -> Response {
    let history: CheckpointHistoryDto = match state.engine.job_checkpoints(&job_id).await {
        Ok(history) => history,
        Err(e) => return error_response(&e),
    };
    Json(history).into_response()
}

fn parse_format(value: Option<&str>) -> Result<seatunnel_config::ConfigFormat, String> {
    match value.map(str::to_ascii_lowercase).as_deref() {
        None | Some("yaml") | Some("yml") => Ok(seatunnel_config::ConfigFormat::YAML),
        Some("toml") => Ok(seatunnel_config::ConfigFormat::TOML),
        Some("hocon") | Some("conf") => Ok(seatunnel_config::ConfigFormat::HOCON),
        Some(other) => Err(format!(
            "unsupported format '{}' (expected yaml, toml or hocon)",
            other
        )),
    }
}

fn bad_request(message: String) -> Response {
    error_response(&EngineError::Invalid(message))
}
