/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.
 */

//! REST handlers for the web console.

pub mod auth;
pub mod jobs;
pub mod logs;

use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::middleware::{self, Next};
use axum::response::{IntoResponse, Response};
use axum::{Json, Router};
use std::collections::HashSet;
use std::time::Instant;

use crate::dto::{
    ClusterInfoDto, ErrorDto, HealthDto, OverviewDto, WorkerDetailDto, WorkerTaskDto,
};
use crate::{AppState, EngineError};

/// `GET /api/v1/health` — web liveness plus master reachability probe.
pub async fn health(State(state): State<AppState>) -> Response {
    match state.engine.cluster_info().await {
        Ok(_) => (
            StatusCode::OK,
            Json(HealthDto {
                status: "ok",
                master: state.master_label.clone(),
                error: String::new(),
            }),
        )
            .into_response(),
        Err(e) => (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(HealthDto {
                status: "degraded",
                master: state.master_label.clone(),
                error: e.to_string(),
            }),
        )
            .into_response(),
    }
}

/// `GET /api/v1/overview` — dashboard aggregation (job counts + cluster).
pub async fn overview(State(state): State<AppState>) -> Response {
    let (jobs, cluster) = match (
        state.engine.list_jobs().await,
        state.engine.cluster_info().await,
    ) {
        (Ok(jobs), Ok(cluster)) => (jobs, cluster),
        (Err(e), _) | (_, Err(e)) => return error_response(&e),
    };

    let mut overview = OverviewDto {
        jobs_total: jobs.len() as i64,
        jobs_running: 0,
        jobs_pending: 0,
        jobs_completed: 0,
        jobs_failed: 0,
        jobs_cancelled: 0,
        jobs_by_state: Default::default(),
        cluster: ClusterInfoDto {
            leader_id: cluster.leader_id.clone(),
            leader_term: cluster.leader_term,
            leader_role: cluster.leader_role.clone(),
            available_workers: cluster.available_workers,
            total_tasks: cluster.total_tasks,
            running_tasks: cluster.running_tasks,
            raft_members: cluster.raft_members,
            workers: cluster.workers,
        },
    };
    for job in jobs {
        *overview.jobs_by_state.entry(job.state.clone()).or_default() += 1;
        match job.state.as_str() {
            "RUNNING" => overview.jobs_running += 1,
            "COMPLETED" => overview.jobs_completed += 1,
            "FAILED" => overview.jobs_failed += 1,
            "CANCELLED" => overview.jobs_cancelled += 1,
            "CREATED" | "SCHEDULED" => overview.jobs_pending += 1,
            _ => {}
        }
    }
    Json(overview).into_response()
}

/// `GET /api/v1/cluster` — workers and leader info.
pub async fn cluster(State(state): State<AppState>) -> Response {
    match state.engine.cluster_info().await {
        Ok(info) => Json(info).into_response(),
        Err(e) => error_response(&e),
    }
}

/// `GET /api/v1/cluster/history` — console-side sampled worker/cluster
/// time series. Web-process-local: empty until the poller has cycled.
pub async fn cluster_history(State(state): State<AppState>) -> Response {
    Json(state.history.cluster_snapshot()).into_response()
}

/// `GET /api/v1/cluster/workers/{worker_id}` — one worker plus the task
/// summaries it currently owns (task ids joined with job statuses).
pub async fn worker_detail(
    State(state): State<AppState>,
    Path(worker_id): Path<String>,
) -> Response {
    let info = match state.engine.cluster_info().await {
        Ok(info) => info,
        Err(e) => return error_response(&e),
    };
    let Some(worker) = info
        .workers
        .into_iter()
        .find(|w| w.worker_id == worker_id)
    else {
        return error_response(&EngineError::NotFound(format!(
            "worker {} not found",
            worker_id
        )));
    };
    let owned: HashSet<String> = worker.task_ids.iter().cloned().collect();
    let mut tasks = Vec::new();
    if let Ok(jobs) = state.engine.list_jobs().await {
        for job in jobs {
            let Ok(status) = state.engine.job_status(&job.job_id).await else {
                continue;
            };
            for task in status.tasks {
                if owned.contains(&task.task_id) {
                    tasks.push(WorkerTaskDto {
                        job_id: status.job_id.clone(),
                        job_name: status.job_name.clone(),
                        task_id: task.task_id,
                        state: task.state,
                        processed_records: task.processed_records,
                        records_per_sec: task.records_per_sec,
                        idle_ms: task.idle_ms,
                    });
                }
            }
        }
    }
    tasks.sort_by(|a, b| a.task_id.cmp(&b.task_id));
    Json(WorkerDetailDto { worker, tasks }).into_response()
}

/// `GET /metrics` — Prometheus text exposition.
pub async fn metrics(State(state): State<AppState>) -> Response {
    match state.metrics.gather() {
        Some(body) => (
            StatusCode::OK,
            [(
                axum::http::header::CONTENT_TYPE,
                "text/plain; version=0.0.4",
            )],
            body,
        )
            .into_response(),
        // A missing encode only happens on registry misuse; report 500.
        None => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(ErrorDto {
                error: "metrics encoding failed".to_string(),
            }),
        )
            .into_response(),
    }
}

/// Wrap the router with request counting and latency histograms.
pub fn http_middleware(app: Router, state: AppState) -> Router {
    app.layer(middleware::from_fn_with_state(state, http_metrics_mw))
}

async fn http_metrics_mw(
    State(state): State<AppState>,
    request: axum::extract::Request,
    next: Next,
) -> Response {
    let method = request.method().to_string();
    let start = Instant::now();
    let response = next.run(request).await;
    state.metrics.record_http(
        &method,
        response.status().as_u16(),
        start.elapsed().as_secs_f64(),
    );
    response
}

/// Map an [`EngineError`] to its HTTP status with a JSON error body.
pub fn error_response(e: &EngineError) -> Response {
    (
        e.http_status(),
        Json(ErrorDto {
            error: e.to_string(),
        }),
    )
        .into_response()
}
