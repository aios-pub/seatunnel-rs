/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.
 */

//! Embedded SPA assets served from the Leptos frontend build output
//! (`../ui/dist`, produced by trunk and committed so plain
//! `cargo build` needs no frontend toolchain).

use axum::http::{StatusCode, Uri, header};
use axum::response::{IntoResponse, Response};
use rust_embed::RustEmbed;

#[derive(RustEmbed)]
#[folder = "../ui/dist"]
struct Assets;

/// Serve a static asset, falling back to `index.html` for SPA routes.
pub async fn static_handler(uri: Uri) -> Response {
    let path = uri.path().trim_start_matches('/');
    if path.is_empty() {
        return index_response();
    }
    match Assets::get(path) {
        Some(asset) => asset_response(path, &asset),
        // Unknown non-API paths go to the SPA entry (client-side routing).
        None => index_response(),
    }
}

fn index_response() -> Response {
    match Assets::get("index.html") {
        Some(asset) => asset_response("index.html", &asset),
        None => (
            StatusCode::NOT_FOUND,
            "frontend bundle missing; build the UI crate (seatunnel-web/ui) with trunk",
        )
            .into_response(),
    }
}

fn asset_response(path: &str, asset: &rust_embed::EmbeddedFile) -> Response {
    let mime = mime_guess::from_path(path).first_or_octet_stream();
    // trunk emits content-hashed file names for everything except the SPA
    // entry, so only index.html (and SPA routes falling back to it) may be
    // revalidated; every other asset can be cached forever.
    let cache_control = if path == "index.html" {
        "no-cache"
    } else {
        "public, max-age=31536000, immutable"
    };
    (
        [
            (header::CONTENT_TYPE, mime.as_ref()),
            (header::CACHE_CONTROL, cache_control),
        ],
        asset.data.clone(),
    )
        .into_response()
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::Router;
    use axum::body::Body;
    use axum::http::Request;
    use tower::ServiceExt;

    #[tokio::test]
    async fn index_and_spa_fallback_ask_for_revalidation() {
        let app = Router::new().fallback(static_handler);
        for uri in ["/", "/jobs/any-client-route"] {
            let response = app
                .clone()
                .oneshot(Request::builder().uri(uri).body(Body::empty()).unwrap())
                .await
                .unwrap();
            assert_eq!(response.status(), StatusCode::OK, "{uri}");
            assert_eq!(
                response.headers()["cache-control"],
                "no-cache",
                "{uri} serves index.html and must be revalidated"
            );
        }
    }

    #[tokio::test]
    async fn hashed_wasm_asset_is_immutable_and_brotli_compressed() {
        let name = Assets::iter()
            .find(|path| path.ends_with("_bg.wasm"))
            .expect("trunk wasm bundle present in ui/dist");
        let app = Router::new()
            .fallback(static_handler)
            .layer(tower_http::compression::CompressionLayer::new());
        let response = app
            .oneshot(
                Request::builder()
                    .uri(format!("/{name}"))
                    .header("accept-encoding", "br")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(
            response.headers()["content-encoding"], "br",
            "the wasm bundle dominates page weight and must be compressed"
        );
        assert_eq!(
            response.headers()["cache-control"],
            "public, max-age=31536000, immutable"
        );
    }
}
