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
pub use metrics::{Metrics, spawn_poller};

/// Shared application state for handlers.
#[derive(Clone)]
pub struct AppState {
    /// Engine operations (gRPC `EngineClient` in production, fakes in tests).
    pub engine: Arc<dyn EngineOps>,
    /// Prometheus registry and metric handles.
    pub metrics: Arc<metrics::Metrics>,
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
                "/api/v1/jobs/{job_id}/cancel",
                axum::routing::post(api::jobs::cancel_job),
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
            .route("/api/v1/cluster", get(api::cluster))
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
) -> tokio::task::JoinHandle<()> {
    let state = AppState {
        engine: Arc::new(seatunnel_engine_client::EngineClient::new(&master)),
        metrics: Arc::new(Metrics::new()),
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
        AppState {
            engine: Arc::new(engine),
            metrics: Arc::new(metrics::Metrics::new()),
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
        response.status()
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
    async fn metrics_exposes_job_gauges() {
        let state = test_state(FakeEngine::with_running_job());
        // Refresh gauges once synchronously.
        state.metrics.refresh(&*state.engine).await;
        let (status, body) = get_json(&state, "/metrics", None).await;
        assert_eq!(status, StatusCode::OK);
        assert!(body.contains("seatunnel_jobs"));
        assert!(body.contains("seatunnel_workers"));
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
