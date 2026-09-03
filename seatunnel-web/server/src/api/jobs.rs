/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.
 */

//! Job management handlers.

use std::collections::HashMap;

use axum::Json;
use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::response::sse::{Event, KeepAlive, Sse};
use axum::response::{IntoResponse, Response};
use futures::stream::Stream;
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

/// Read `env.job.name` from a job config — the display name pipelines set
/// in their config (nested `env.job.name` or the flat dotted key
/// `env."job.name"`). Mirrors the CLI's submit/update lookup so the web
/// console names jobs the same way when the caller left the name empty.
pub(crate) fn env_job_name(config: &serde_json::Value) -> Option<String> {
    let env = config.get("env")?;
    let nested = env
        .get("job")
        .and_then(|job| job.get("name"))
        .and_then(|n| n.as_str());
    let flat = env.get("job.name").and_then(|n| n.as_str());
    nested
        .or(flat)
        .map(str::trim)
        .filter(|name| !name.is_empty())
        .map(str::to_string)
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
    let config = serde_json::Value::Object(doc);
    let config_bytes = match serde_json::to_vec(&config) {
        Ok(bytes) => bytes,
        Err(e) => return bad_request(format!("config serialization error: {}", e)),
    };

    let job_id = format!("job-{}", Uuid::new_v4());
    // Without an explicit name, the config's own `env.job.name` wins over
    // the synthetic default (same as the CLI submit path).
    let mut request = request;
    if request.job_name.is_none() {
        request.job_name = env_job_name(&config);
    }
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
    let config = match body.config_text.trim() {
        "" => {
            return error_response(&EngineError::Invalid(
                "config_text must not be empty".to_string(),
            ));
        }
        text => match serde_json::from_str::<serde_json::Value>(text) {
            Ok(value) => value,
            Err(e) => return error_response(&EngineError::Invalid(format!("invalid JSON config: {}", e))),
        },
    };
    let config_bytes = match serde_json::to_vec(&config) {
        Ok(bytes) => bytes,
        Err(e) => return error_response(&EngineError::Invalid(e.to_string())),
    };
    // Name resolution, mirroring the CLI update path: an explicit override
    // wins, then the edited config's own `env.job.name`. When neither is
    // present keep the name the job already has — an update changes the
    // config, not the job's identity — and only a nameless, unknown job
    // falls back to the job id.
    let mut job_name = body.job_name.clone().or_else(|| env_job_name(&config));
    if job_name.is_none() {
        if let Ok(status) = state.engine.job_status(&job_id).await {
            job_name = Some(status.job_name);
        }
    }
    let job_name = job_name.unwrap_or_else(|| job_id.clone());
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

/// `GET /api/v1/jobs/{job_id}/history` — console-side sampled time series
/// (throughput / sink latency per task). Web-process-local: empty until the
/// poller has seen the job.
pub async fn job_history(State(state): State<AppState>, Path(job_id): Path<String>) -> Response {
    Json(state.history.job_snapshot(&job_id)).into_response()
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

// --- Live log stream (SSE) ---------------------------------------------------

/// Server-side poll cadence for the log stream: the engine itself ships
/// task logs with 2 s heartbeats, so 1 s here bounds console latency to
/// roughly the engine's own granularity.
const LOG_STREAM_POLL: std::time::Duration = std::time::Duration::from_secs(1);
/// Per-cycle gRPC budget; a hung master skips the cycle instead of
/// wedging the stream.
const LOG_STREAM_RPC_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(2);
/// Total stream lifetime; the browser's EventSource reconnects and
/// receives a fresh full snapshot.
const LOG_STREAM_MAX_LIFETIME: std::time::Duration = std::time::Duration::from_secs(600);

/// One streamed log update for a task: the new lines since the previous
/// event (`reset` = replace whatever the client has).
#[derive(serde::Serialize)]
struct TaskLogDelta {
    task_id: String,
    lines: Vec<String>,
    reset: bool,
}

/// Largest-overlap delta between the previously seen tail and the current
/// ring tail: returns `k` such that `new[..k] == prev[prev.len()-k..]` and
/// the new lines are `new[k..]`. Plain appends give `k = prev.len()`; a
/// ring-buffer shift still yields exactly the freshly appended lines.
/// Complexity is bounded by the 500-line ring, and it only runs when the
/// content changed.
fn tail_delta<'a>(prev: &[String], new: &'a [String]) -> &'a [String] {
    if prev.is_empty() {
        return new;
    }
    let max_overlap = prev.len().min(new.len());
    for k in (0..=max_overlap).rev() {
        if new[..k] == prev[prev.len() - k..] {
            return &new[k..];
        }
    }
    new
}

fn sse_error_event(message: &str) -> Event {
    Event::default().data(serde_json::json!({ "error": message }).to_string())
}

/// `GET /api/v1/jobs/{job_id}/logs/stream` — Server-Sent Events stream of
/// per-task log deltas (the live-log viewer's transport). Ends after
/// `LOG_STREAM_MAX_LIFETIME`; the browser reconnects automatically and the
/// first event of every connection is a full snapshot.
pub async fn job_logs_stream(
    State(state): State<AppState>,
    Path(job_id): Path<String>,
) -> Sse<impl Stream<Item = Result<Event, std::convert::Infallible>>> {
    let engine = state.engine.clone();
    let stream = async_stream::stream! {
        yield Ok(Event::default().retry(std::time::Duration::from_secs(2)));
        let mut prev: HashMap<String, Vec<String>> = HashMap::new();
        let started = tokio::time::Instant::now();
        loop {
            if started.elapsed() > LOG_STREAM_MAX_LIFETIME {
                break;
            }
            tokio::time::sleep(LOG_STREAM_POLL).await;
            let logs = match tokio::time::timeout(
                LOG_STREAM_RPC_TIMEOUT,
                engine.job_logs(&job_id),
            )
            .await
            {
                Ok(Ok(logs)) => logs,
                Ok(Err(EngineError::NotFound(msg))) => {
                    // Job finished/evicted: logs are final, end the stream.
                    yield Ok(sse_error_event(&format!("stream closed: {msg}")));
                    break;
                }
                Ok(Err(e)) => {
                    yield Ok(sse_error_event(&e.to_string()));
                    continue;
                }
                Err(_) => {
                    yield Ok(sse_error_event("engine read timed out"));
                    continue;
                }
            };
            for task in &logs.tasks {
                let previous = prev.get(&task.task_id).map(|v| v.as_slice()).unwrap_or(&[]);
                let delta = tail_delta(previous, &task.lines);
                if delta.is_empty() && !previous.is_empty() {
                    continue;
                }
                let reset = previous.is_empty() || delta.len() == task.lines.len();
                prev.insert(task.task_id.clone(), task.lines.clone());
                if delta.is_empty() {
                    continue;
                }
                let event = TaskLogDelta {
                    task_id: task.task_id.clone(),
                    lines: delta.to_vec(),
                    reset,
                };
                yield Ok(Event::default().data(
                    serde_json::to_string(&event).unwrap_or_default(),
                ));
            }
        }
    };
    Sse::new(stream).keep_alive(KeepAlive::new().interval(std::time::Duration::from_secs(15)))
}

#[cfg(test)]
mod tests {
    use super::tail_delta;

    fn lines(start: usize, count: usize) -> Vec<String> {
        (start..start + count).map(|i| format!("line-{i}")).collect()
    }

    #[test]
    fn plain_append_yields_only_new_lines() {
        let prev = lines(0, 10);
        let new = lines(0, 15);
        assert_eq!(tail_delta(&prev, &new), lines(10, 5));
    }

    #[test]
    fn no_change_yields_empty() {
        let prev = lines(0, 10);
        assert!(tail_delta(&prev, &prev.clone()).is_empty());
    }

    #[test]
    fn ring_shift_yields_shifted_in_lines() {
        // 500-cap ring: 5 new lines push 5 old ones out.
        let prev = lines(0, 100);
        let mut new = lines(5, 95);
        new.extend(lines(100, 5));
        assert_eq!(tail_delta(&prev, &new), lines(100, 5));
    }

    #[test]
    fn full_rewrite_sends_everything() {
        let prev = lines(0, 10);
        let new = lines(1000, 10);
        assert_eq!(tail_delta(&prev, &new), new);
    }

    #[test]
    fn empty_prev_sends_everything() {
        let new = lines(0, 3);
        assert_eq!(tail_delta(&[], &new), new);
    }

    #[test]
    fn env_job_name_nested_form() {
        let config = serde_json::json!({
            "env": { "job": { "name": "user-role-rabbitmq" } },
            "source": [], "sink": []
        });
        assert_eq!(
            super::env_job_name(&config).as_deref(),
            Some("user-role-rabbitmq")
        );
    }

    #[test]
    fn env_job_name_flat_dotted_key() {
        // HOCON/YAML writers may emit the dotted key instead of nesting.
        let config = serde_json::json!({
            "env": { "job.name": "recommand" },
            "source": [], "sink": []
        });
        assert_eq!(super::env_job_name(&config).as_deref(), Some("recommand"));
    }

    #[test]
    fn env_job_name_absent_or_blank_yields_none() {
        assert_eq!(super::env_job_name(&serde_json::json!({ "env": {} })), None);
        assert_eq!(
            super::env_job_name(&serde_json::json!({ "env": { "job": { "name": "  " } } })),
            None
        );
        assert_eq!(super::env_job_name(&serde_json::json!({})), None);
    }
}
