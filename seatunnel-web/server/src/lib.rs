/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.
 */

//! Web management console for SeaTunnel clusters.
//!
//! Serves a REST API (`/api/v1/*`), a Prometheus `/metrics` endpoint and
//! the embedded Leptos single-page UI (built from `../ui`).
//! All engine operations go through [`EngineOps`], implemented by the
//! gRPC [`EngineClient`] against the master nodes.

mod api;
mod assets;
mod auth;
mod dto;
mod engine;
mod history;
mod metrics;

use std::sync::Arc;

use axum::Router;
use axum::middleware;
use axum::routing::get;

pub use auth::AuthConfig;
pub use dto::{
    CheckpointEntryDto, CheckpointHistoryDto, ClusterInfoDto, ErrorDto, JobStatusDto,
    JobSummaryDto, SubmitJobDto, SubmitResultDto, TaskCheckpointDto, TaskStatusDto, WorkerDto,
};
pub use engine::{EngineError, EngineOps};
pub use history::History;
pub use metrics::{Metrics, spawn_poller};

/// Shared application state for handlers.
#[derive(Clone)]
pub struct AppState {
    /// Engine operations (gRPC `EngineClient` in production, fakes in tests).
    pub engine: Arc<dyn EngineOps>,
    /// Prometheus registry and metric handles.
    pub metrics: Arc<metrics::Metrics>,
    /// Time-series ring backing the console's charts.
    pub history: Arc<history::History>,
    /// Node log directory for the online log viewer (`None` hides the
    /// feature; the endpoints answer 404 with a hint).
    pub log_dir: Option<String>,
    /// Display-only master address list this console is bound to.
    pub master_label: String,
    /// Login credentials and session-signing settings.
    pub auth: Arc<AuthConfig>,
    /// Previous `(processed_records, sampled_at_ms)` per task, used to
    /// derive throughput between consecutive status reads.
    pub task_samples: Arc<std::sync::Mutex<std::collections::HashMap<String, (i64, i64)>>>,
}

/// Build the full web console router (REST API + metrics + embedded SPA).
pub fn build_router(state: AppState) -> Router {
    api::http_middleware(
        Router::new()
            .route("/api/v1/health", get(api::health))
            .route("/api/v1/login", axum::routing::post(api::auth::login))
            .route("/api/v1/logout", axum::routing::post(api::auth::logout))
            .route("/api/v1/whoami", get(api::auth::whoami))
            .route("/api/v1/overview", get(api::overview))
            .route(
                "/api/v1/jobs",
                get(api::jobs::list_jobs).post(api::jobs::submit_job),
            )
            .route("/api/v1/jobs/{job_id}", get(api::jobs::job_detail))
            .route(
                "/api/v1/jobs/{job_id}",
                axum::routing::delete(api::jobs::delete_job),
            )
            .route(
                "/api/v1/jobs/{job_id}/cancel",
                axum::routing::post(api::jobs::cancel_job),
            )
            .route(
                "/api/v1/jobs/{job_id}/restart",
                axum::routing::post(api::jobs::restart_job),
            )
            .route(
                "/api/v1/jobs/{job_id}/update",
                axum::routing::post(api::jobs::update_job),
            )
            .route(
                "/api/v1/jobs/{job_id}/checkpoints",
                get(api::jobs::job_checkpoints),
            )
            .route("/api/v1/jobs/{job_id}/logs", get(api::jobs::job_logs))
            .route(
                "/api/v1/jobs/{job_id}/logs/stream",
                get(api::jobs::job_logs_stream),
            )
            .route(
                "/api/v1/jobs/{job_id}/history",
                get(api::jobs::job_history),
            )
            .route("/api/v1/cluster", get(api::cluster))
            .route(
                "/api/v1/cluster/workers/{worker_id}",
                get(api::worker_detail),
            )
            .route("/api/v1/cluster/history", get(api::cluster_history))
            .route("/api/v1/logs/files", get(api::logs::log_files))
            .route("/api/v1/logs/files/{name}", get(api::logs::log_file))
            .route(
                "/api/v1/logs/files/{name}/stream",
                get(api::logs::log_file_stream),
            )
            .route("/metrics", get(api::metrics))
            .fallback(assets::static_handler)
            .with_state(state.clone())
            // Auth sits inside the metrics layer so 401s are still counted.
            .layer(middleware::from_fn_with_state(
                state.clone(),
                api::auth::auth_middleware,
            )),
        state,
    )
}

/// Launch the console as a detached task: app state, metrics poller and
/// `axum::serve` on `listen`, proxying to the engine at `master`
/// (host:port, comma separated for failover). Used by `--web` on the
/// engine server binary; the standalone `seatunnel-web` binary wires the
/// same pieces itself to expose every knob (refresh interval, session
/// TTL). A bind failure only logs an error — the host process keeps
/// running.
pub fn spawn_console(
    listen: String,
    master: String,
    auth: AuthConfig,
    log_dir: Option<String>,
) -> tokio::task::JoinHandle<()> {
    let state = AppState {
        engine: Arc::new(seatunnel_engine_client::EngineClient::new(&master)),
        metrics: Arc::new(Metrics::new()),
        history: Arc::new(History::new(history::DEFAULT_CAPACITY)),
        log_dir,
        master_label: master.clone(),
        auth: Arc::new(auth),
        task_samples: Arc::default(),
    };
    spawn_poller(state.clone(), std::time::Duration::from_secs(5));
    let app = build_router(state);
    tokio::spawn(async move {
        match tokio::net::TcpListener::bind(&listen).await {
            Ok(listener) => {
                tracing::info!(
                    "web console listening on http://{} (engine: {})",
                    listen,
                    master
                );
                if let Err(e) = axum::serve(listener, app).await {
                    tracing::error!("web console stopped: {}", e);
                }
            }
            Err(e) => tracing::error!("web console cannot bind {}: {}", listen, e),
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use engine::FakeEngine;
    use http_body_util::BodyExt;
    use tower::ServiceExt;

    fn test_state(engine: FakeEngine) -> AppState {
        test_state_shared(Arc::new(engine))
    }

    /// Like [`test_state`], but hands back the shared fake so tests can
    /// assert on what the handlers passed to the engine.
    fn test_state_shared(engine: Arc<FakeEngine>) -> AppState {
        AppState {
            engine,
            metrics: Arc::new(metrics::Metrics::new()),
            history: Arc::new(history::History::new(history::DEFAULT_CAPACITY)),
            log_dir: None,
            master_label: "127.0.0.1:5800".to_string(),
            auth: Arc::new(AuthConfig::new(
                "admin".to_string(),
                "test-pass".to_string(),
                3600,
            )),
            task_samples: Arc::default(),
        }
    }

    /// Login and return the `name=value` session cookie pair.
    async fn login_cookie(state: &AppState) -> String {
        let response = build_router(state.clone())
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/v1/login")
                    .header("content-type", "application/json")
                    .body(Body::from(
                        serde_json::json!({ "username": "admin", "password": "test-pass" })
                            .to_string(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        response
            .headers()
            .get("set-cookie")
            .expect("login sets a session cookie")
            .to_str()
            .unwrap()
            .split(';')
            .next()
            .unwrap()
            .to_string()
    }

    async fn get_json(state: &AppState, path: &str, cookie: Option<&str>) -> (StatusCode, String) {
        let mut builder = Request::builder().uri(path);
        if let Some(cookie) = cookie {
            builder = builder.header("cookie", cookie);
        }
        let response = build_router(state.clone())
            .oneshot(builder.body(Body::empty()).unwrap())
            .await
            .unwrap();
        let status = response.status();
        let body = response
            .into_body()
            .collect()
            .await
            .unwrap()
            .to_bytes()
            .to_ascii_lowercase();
        let body = String::from_utf8(body).unwrap();
        (status, body)
    }

    async fn post_json(
        state: &AppState,
        path: &str,
        body: String,
        cookie: Option<&str>,
    ) -> StatusCode {
        post_json_body(state, path, body, cookie).await.0
    }

    /// `post_json` that also returns the response body (lowercased).
    async fn post_json_body(
        state: &AppState,
        path: &str,
        body: String,
        cookie: Option<&str>,
    ) -> (StatusCode, String) {
        let mut builder = Request::builder()
            .method("POST")
            .uri(path)
            .header("content-type", "application/json");
        if let Some(cookie) = cookie {
            builder = builder.header("cookie", cookie);
        }
        let response = build_router(state.clone())
            .oneshot(builder.body(Body::from(body)).unwrap())
            .await
            .unwrap();
        let status = response.status();
        let bytes = response.into_body().collect().await.unwrap().to_bytes();
        let body = String::from_utf8(bytes.to_vec()).unwrap();
        (status, body)
    }

    #[tokio::test]
    async fn health_ok_when_master_reachable() {
        let state = test_state(FakeEngine::default());
        let (status, body) = get_json(&state, "/api/v1/health", None).await;
        assert_eq!(status, StatusCode::OK);
        assert!(body.contains("\"status\":\"ok\""));
    }

    #[tokio::test]
    async fn health_degraded_when_master_unreachable() {
        let state = test_state(FakeEngine::unreachable());
        let (status, body) = get_json(&state, "/api/v1/health", None).await;
        assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
        assert!(body.contains("degraded"));
    }

    #[tokio::test]
    async fn protected_api_requires_login() {
        let state = test_state(FakeEngine::default());
        for path in [
            "/api/v1/jobs",
            "/api/v1/overview",
            "/api/v1/cluster",
            "/api/v1/whoami",
        ] {
            let (status, _) = get_json(&state, path, None).await;
            assert_eq!(
                status,
                StatusCode::UNAUTHORIZED,
                "{} should require auth",
                path
            );
        }
    }

    #[tokio::test]
    async fn login_with_wrong_password_is_rejected() {
        let state = test_state(FakeEngine::default());
        let response = build_router(state)
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/v1/login")
                    .header("content-type", "application/json")
                    .body(Body::from(
                        serde_json::json!({ "username": "admin", "password": "wrong" }).to_string(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
        assert!(response.headers().get("set-cookie").is_none());
    }

    #[tokio::test]
    async fn login_grants_access_and_reports_identity() {
        let state = test_state(FakeEngine::default());
        let cookie = login_cookie(&state).await;
        let (status, body) = get_json(&state, "/api/v1/jobs", Some(&cookie)).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body, "[]");
        let (status, body) = get_json(&state, "/api/v1/whoami", Some(&cookie)).await;
        assert_eq!(status, StatusCode::OK);
        assert!(body.contains("admin"));
    }

    #[tokio::test]
    async fn logout_clears_the_session_cookie() {
        let state = test_state(FakeEngine::default());
        let status = post_json(&state, "/api/v1/logout", String::new(), None).await;
        assert_eq!(status, StatusCode::OK);
        let response = build_router(state)
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/v1/logout")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        let cookie = response
            .headers()
            .get("set-cookie")
            .and_then(|v| v.to_str().ok())
            .unwrap_or_default()
            .to_string();
        assert!(
            cookie.to_ascii_lowercase().contains("max-age=0"),
            "logout must expire the cookie"
        );
    }

    #[tokio::test]
    async fn tampered_session_cookie_is_rejected() {
        let state = test_state(FakeEngine::default());
        let (status, _) =
            get_json(&state, "/api/v1/jobs", Some("seatunnel_session=1:deadbeef")).await;
        assert_eq!(status, StatusCode::UNAUTHORIZED);
    }

    #[tokio::test]
    async fn public_assets_stay_open() {
        let state = test_state(FakeEngine::default());
        // SPA entry and health/metrics remain reachable without a session.
        let (index_status, body) = get_json(&state, "/", None).await;
        assert_eq!(index_status, StatusCode::OK);
        assert!(body.contains("seatunnel console") || body.contains("<html"));
        let (status, _) = get_json(&state, "/metrics", None).await;
        assert_eq!(status, StatusCode::OK);
    }

    #[tokio::test]
    async fn submit_rejects_invalid_config() {
        let state = test_state(FakeEngine::default());
        let cookie = login_cookie(&state).await;
        let status = post_json(
            &state,
            "/api/v1/jobs",
            serde_json::json!({ "config_text": "not: [valid: yaml" }).to_string(),
            Some(&cookie),
        )
        .await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn submit_rejects_config_without_source() {
        let state = test_state(FakeEngine::default());
        let cookie = login_cookie(&state).await;
        let status = post_json(
            &state,
            "/api/v1/jobs",
            serde_json::json!({
                "config_text": "env { parallelism = 1 }\nsource []\nsink []"
            })
            .to_string(),
            Some(&cookie),
        )
        .await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn submit_valid_yaml_creates_job() {
        let state = test_state(FakeEngine::default());
        let cookie = login_cookie(&state).await;
        let config = "env:\n  job.name: e2e\nsource:\n  Fake:\n    rows: 10\nsink:\n  Console: {}";
        let status = post_json(
            &state,
            "/api/v1/jobs",
            serde_json::json!({ "config_text": config, "job_name": "e2e" }).to_string(),
            Some(&cookie),
        )
        .await;
        assert_eq!(status, StatusCode::OK);

        let (status, body) = get_json(&state, "/api/v1/jobs", Some(&cookie)).await;
        assert_eq!(status, StatusCode::OK);
        assert!(body.contains("e2e"));
    }

    #[tokio::test]
    async fn unknown_job_returns_404() {
        let state = test_state(FakeEngine::default());
        let cookie = login_cookie(&state).await;
        let (status, _) = get_json(&state, "/api/v1/jobs/missing", Some(&cookie)).await;
        assert_eq!(status, StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn cancel_unknown_job_returns_404() {
        let state = test_state(FakeEngine::default());
        let cookie = login_cookie(&state).await;
        let status = post_json(
            &state,
            "/api/v1/jobs/missing/cancel",
            String::new(),
            Some(&cookie),
        )
        .await;
        assert_eq!(status, StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn restart_unknown_job_returns_404() {
        let state = test_state(FakeEngine::default());
        let cookie = login_cookie(&state).await;
        let status = post_json(
            &state,
            "/api/v1/jobs/missing/restart",
            String::new(),
            Some(&cookie),
        )
        .await;
        assert_eq!(status, StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn restart_known_job_returns_result() {
        let state = test_state(FakeEngine::with_running_job());
        let cookie = login_cookie(&state).await;
        let (status, body) = post_json_body(
            &state,
            "/api/v1/jobs/job-1/restart",
            String::new(),
            Some(&cookie),
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        assert!(body.contains("restarted"), "body: {}", body);
    }

    #[tokio::test]
    async fn update_names_job_after_config_env_job_name() {
        let fake = Arc::new(FakeEngine::with_running_job());
        let state = test_state_shared(fake.clone());
        let cookie = login_cookie(&state).await;
        let config = serde_json::json!({
            "env": { "job": { "name": "user-role-rabbitmq" } },
            "source": [],
            "sink": []
        });
        let (status, body) = post_json_body(
            &state,
            "/api/v1/jobs/job-1/update",
            serde_json::json!({ "config_text": config.to_string() }).to_string(),
            Some(&cookie),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "body: {}", body);
        assert_eq!(
            *fake.last_update_name.lock().unwrap(),
            Some("user-role-rabbitmq".to_string())
        );
    }

    #[tokio::test]
    async fn update_without_any_name_keeps_the_current_one() {
        let fake = Arc::new(FakeEngine::with_running_job());
        let state = test_state_shared(fake.clone());
        let cookie = login_cookie(&state).await;
        let config = serde_json::json!({ "env": {}, "source": [], "sink": [] });
        let (status, body) = post_json_body(
            &state,
            "/api/v1/jobs/job-1/update",
            serde_json::json!({ "config_text": config.to_string() }).to_string(),
            Some(&cookie),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "body: {}", body);
        // No explicit name, no env.job.name: the job keeps its name
        // instead of being renamed to the job id.
        assert_eq!(
            *fake.last_update_name.lock().unwrap(),
            Some("demo".to_string())
        );
    }

    /// `delete_json` — DELETE with the session cookie; returns the body.
    async fn delete_json(
        state: &AppState,
        path: &str,
        cookie: Option<&str>,
    ) -> (StatusCode, String) {
        let mut builder = Request::builder().method("DELETE").uri(path);
        if let Some(cookie) = cookie {
            builder = builder.header("cookie", cookie);
        }
        let response = build_router(state.clone())
            .oneshot(builder.body(Body::empty()).unwrap())
            .await
            .unwrap();
        let status = response.status();
        let bytes = response.into_body().collect().await.unwrap().to_bytes();
        (status, String::from_utf8(bytes.to_vec()).unwrap())
    }

    fn fake_engine_with_state(state_name: &str) -> FakeEngine {
        let fake = FakeEngine::default();
        fake.jobs.lock().unwrap().push(JobStatusDto {
            job_id: "job-1".to_string(),
            job_name: "demo".to_string(),
            state: state_name.to_string(),
            start_time_ms: 1,
            end_time_ms: 2,
            error_message: String::new(),
            checkpoint_interval_ms: 10_000,
            checkpoints_completed: 1,
            job_config: "{}".to_string(),
            parallelism: 1,
            tasks: Vec::new(),
        });
        fake
    }

    #[tokio::test]
    async fn delete_unknown_job_returns_404() {
        let state = test_state(FakeEngine::default());
        let cookie = login_cookie(&state).await;
        let (status, _) = delete_json(&state, "/api/v1/jobs/missing", Some(&cookie)).await;
        assert_eq!(status, StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn delete_running_job_returns_400() {
        let state = test_state(fake_engine_with_state("RUNNING"));
        let cookie = login_cookie(&state).await;
        let (status, _) = delete_json(&state, "/api/v1/jobs/job-1", Some(&cookie)).await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn delete_terminal_job_removes_it() {
        let state = test_state(fake_engine_with_state("CANCELLED"));
        let cookie = login_cookie(&state).await;
        let (status, body) = delete_json(&state, "/api/v1/jobs/job-1", Some(&cookie)).await;
        assert_eq!(status, StatusCode::OK, "body: {}", body);
        assert!(body.contains("\"job-1\""), "body: {}", body);
        // The job is gone from the list.
        let (status, body) = get_json(&state, "/api/v1/jobs", Some(&cookie)).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body, "[]", "body: {}", body);
    }

    #[tokio::test]
    async fn metrics_exposes_job_gauges() {
        let state = test_state(FakeEngine::with_running_job());
        // Refresh gauges once synchronously.
        state.metrics.refresh(&*state.engine, &state.history).await;
        let (status, body) = get_json(&state, "/metrics", None).await;
        assert_eq!(status, StatusCode::OK);
        assert!(body.contains("seatunnel_jobs"));
        assert!(body.contains("seatunnel_workers"));
    }

    #[tokio::test]
    async fn history_endpoint_serves_poller_samples() {
        let state = test_state(FakeEngine::with_running_job());
        let cookie = login_cookie(&state).await;
        state.metrics.refresh(&*state.engine, &state.history).await;
        let (status, body) = get_json(&state, "/api/v1/jobs/job-1/history", Some(&cookie)).await;
        assert_eq!(status, StatusCode::OK);
        assert!(body.contains("records_per_sec"), "body: {}", body);
        let (status, body) = get_json(&state, "/api/v1/cluster/history", Some(&cookie)).await;
        assert_eq!(status, StatusCode::OK);
        assert!(body.contains("cpu_permille"), "body: {}", body);
    }

    #[tokio::test]
    async fn worker_detail_lists_owned_tasks() {
        let state = test_state(FakeEngine::with_running_job());
        let cookie = login_cookie(&state).await;
        let (status, body) =
            get_json(&state, "/api/v1/cluster/workers/worker-1", Some(&cookie)).await;
        assert_eq!(status, StatusCode::OK);
        assert!(body.contains("task-0"), "body: {}", body);
        assert!(body.contains("demo"), "body: {}", body);
        let (status, _) =
            get_json(&state, "/api/v1/cluster/workers/nope", Some(&cookie)).await;
        assert_eq!(status, StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn log_endpoints_serve_node_files() {
        let mut state = test_state(FakeEngine::default());
        let dir = std::env::temp_dir().join(format!("st-web-logs-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::write(
            dir.join("master.2026-09-02.log"),
            "[2026-09-02 10:00:00 INFO seatunnel]: hello\n\
             [2026-09-02 10:00:01 ERROR seatunnel]: boom\n",
        )
        .unwrap();
        state.log_dir = Some(dir.to_string_lossy().to_string());
        let cookie = login_cookie(&state).await;

        let (status, body) = get_json(&state, "/api/v1/logs/files", Some(&cookie)).await;
        assert_eq!(status, StatusCode::OK);
        assert!(body.contains("master.2026-09-02.log"), "body: {}", body);

        let (status, body) = get_json(
            &state,
            "/api/v1/logs/files/master.2026-09-02.log?level=ERROR",
            Some(&cookie),
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        assert!(body.contains("boom"), "body: {}", body);
        assert!(!body.contains("hello"), "level filter failed: {}", body);

        // Path traversal is rejected by the file-name whitelist.
        let (status, _) = get_json(
            &state,
            "/api/v1/logs/files/..%2F..%2Fetc%2Fpasswd",
            Some(&cookie),
        )
        .await;
        assert_ne!(status, StatusCode::OK);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn log_endpoints_404_without_log_dir() {
        let state = test_state(FakeEngine::default());
        let cookie = login_cookie(&state).await;
        let (status, body) = get_json(&state, "/api/v1/logs/files", Some(&cookie)).await;
        assert_eq!(status, StatusCode::NOT_FOUND);
        assert!(body.contains("no log directory"), "body: {}", body);
    }

    #[tokio::test]
    async fn logs_endpoint_returns_lines() {
        let state = test_state(FakeEngine::with_running_job());
        let cookie = login_cookie(&state).await;
        let (status, body) = get_json(&state, "/api/v1/jobs/job-1/logs", Some(&cookie)).await;
        assert_eq!(status, StatusCode::OK);
        assert!(body.contains("task started"));
        assert!(body.contains("record #100"));
    }

    #[tokio::test]
    async fn job_detail_reports_idle_metrics() {
        let state = test_state(FakeEngine::with_running_job());
        let cookie = login_cookie(&state).await;
        let (status, _) = get_json(&state, "/api/v1/jobs/job-1", Some(&cookie)).await;
        assert_eq!(status, StatusCode::OK);
        // Second read derives idle from last_record_ms = 1 (a real sample).
        let (_, body) = get_json(&state, "/api/v1/jobs/job-1", Some(&cookie)).await;
        assert!(body.contains("idle_ms"));
        assert!(body.contains("records_per_sec"));
    }

    #[tokio::test]
    async fn checkpoints_endpoint_returns_history() {
        let state = test_state(FakeEngine::with_running_job());
        let cookie = login_cookie(&state).await;
        let (status, body) =
            get_json(&state, "/api/v1/jobs/job-1/checkpoints", Some(&cookie)).await;
        assert_eq!(status, StatusCode::OK);
        assert!(body.contains("checkpoint_id"));
    }
}
