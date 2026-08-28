/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.
 */

//! Login/logout/whoami handlers and the API authentication middleware.

use axum::extract::State;
use axum::http::{header, HeaderValue, StatusCode};
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};
use axum::Json;
use serde::{Deserialize, Serialize};
use std::time::Duration;

use crate::auth::AuthConfig;
use crate::dto::ErrorDto;
use crate::AppState;

/// Request body for `POST /api/v1/login`.
#[derive(Debug, Deserialize)]
pub struct LoginRequest {
    pub username: String,
    pub password: String,
}

/// Response body for `POST /api/v1/login` and `GET /api/v1/whoami`.
#[derive(Debug, Serialize)]
pub struct WhoamiDto {
    pub username: String,
}

fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

/// `POST /api/v1/login` — verify credentials and set the session cookie.
pub async fn login(State(state): State<AppState>, Json(request): Json<LoginRequest>) -> Response {
    if state.auth.check_credentials(&request.username, &request.password) {
        let token = state.auth.issue_token(now_ms());
        tracing::info!(user = %request.username, "console login succeeded");
        return (
            StatusCode::OK,
            [(
                header::SET_COOKIE,
                HeaderValue::from_str(&state.auth.session_cookie(&token))
                    .expect("static cookie characters only"),
            )],
            Json(WhoamiDto {
                username: request.username,
            }),
        )
            .into_response();
    }
    // Small fixed delay to slow down credential brute-forcing.
    tokio::time::sleep(Duration::from_millis(300)).await;
    tracing::warn!(user = %request.username, "console login failed");
    (
        StatusCode::UNAUTHORIZED,
        Json(ErrorDto {
            error: "invalid username or password".to_string(),
        }),
    )
        .into_response()
}

/// `POST /api/v1/logout` — clear the session cookie.
pub async fn logout() -> Response {
    (
        StatusCode::OK,
        [(header::SET_COOKIE, AuthConfig::clearing_cookie())],
        Json(serde_json::json!({ "logged_out": true })),
    )
        .into_response()
}

/// `GET /api/v1/whoami` — identity of the current session (401 otherwise).
pub async fn whoami(State(state): State<AppState>) -> Response {
    // The middleware guarantees a valid session on this route.
    (
        StatusCode::OK,
        Json(WhoamiDto {
            username: state.auth.username(),
        }),
    )
        .into_response()
}

/// Paths that stay reachable without a session: the login endpoints, the
/// liveness probe and the static SPA assets (the UI code itself carries no
/// data; every data endpoint requires authentication).
fn is_public(path: &str) -> bool {
    if !path.starts_with("/api/") {
        return true;
    }
    matches!(path, "/api/v1/login" | "/api/v1/logout" | "/api/v1/health")
}

/// Reject unauthenticated API calls with 401 before they reach handlers.
pub async fn auth_middleware(
    State(state): State<AppState>,
    request: axum::extract::Request,
    next: Next,
) -> Response {
    if state.auth.disabled || is_public(request.uri().path()) {
        return next.run(request).await;
    }
    let authorized = request
        .headers()
        .get(header::COOKIE)
        .and_then(|value| value.to_str().ok())
        .and_then(AuthConfig::token_from_cookie_header)
        .and_then(|token| state.auth.verify_token(token, now_ms()))
        .is_some();
    if authorized {
        return next.run(request).await;
    }
    (
        StatusCode::UNAUTHORIZED,
        Json(ErrorDto {
            error: "unauthorized".to_string(),
        }),
    )
        .into_response()
}
