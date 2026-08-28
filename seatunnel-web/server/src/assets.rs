/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.
 */

//! Embedded SPA assets served from the Leptos frontend build output
//! (`../ui/dist`, produced by trunk and committed so plain
//! `cargo build` needs no frontend toolchain).

use axum::http::{header, StatusCode, Uri};
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
    ([(header::CONTENT_TYPE, mime.as_ref())], asset.data.clone()).into_response()
}
