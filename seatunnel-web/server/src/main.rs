/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.
 */

//! `seatunnel-web` binary: standalone management console server.

use clap::Parser;
use std::sync::Arc;
use std::time::Duration;

use seatunnel_engine_client::EngineClient;
use seatunnel_web::{build_router, spawn_poller, AppState, AuthConfig, Metrics};

#[derive(Parser)]
#[command(name = "seatunnel-web", about = "SeaTunnel web management console")]
struct Args {
    /// Master addresses, comma separated (failover order).
    #[arg(short, long, default_value = "127.0.0.1:5800")]
    master: String,
    /// HTTP listen address for the console.
    #[arg(short, long, default_value = "0.0.0.0:8080")]
    listen: String,
    /// Engine metrics refresh interval in seconds.
    #[arg(long, default_value_t = 5)]
    refresh_interval_secs: u64,
    /// Console login username.
    #[arg(long, default_value = "admin", env = "SEATUNNEL_WEB_USER")]
    auth_user: String,
    /// Console login password. Falls back to "admin" with a startup
    /// warning when unset; set SEATUNNEL_WEB_PASSWORD in production.
    #[arg(long, env = "SEATUNNEL_WEB_PASSWORD")]
    auth_password: Option<String>,
    /// Session lifetime in minutes.
    #[arg(long, default_value_t = 720)]
    auth_ttl_mins: u64,
    /// Disable authentication entirely (local development only).
    #[arg(long, default_value_t = false)]
    auth_disable: bool,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "info".into()),
        )
        .init();

    let args = Args::parse();

    let auth = if args.auth_disable {
        tracing::warn!("authentication disabled (--auth-disable); console is open to everyone");
        AuthConfig::disabled()
    } else {
        let password = args.auth_password.unwrap_or_else(|| {
            tracing::warn!(
                "no password configured (--auth-password / SEATUNNEL_WEB_PASSWORD); \
                 using the default \"admin\" — change it for anything beyond local use"
            );
            "admin".to_string()
        });
        AuthConfig::new(
            args.auth_user.clone(),
            password,
            args.auth_ttl_mins * 60,
        )
    };

    let state = AppState {
        engine: Arc::new(EngineClient::new(&args.master)),
        metrics: Arc::new(Metrics::new()),
        master_label: args.master.clone(),
        auth: Arc::new(auth),
        task_samples: Arc::default(),
    };
    spawn_poller(state.clone(), Duration::from_secs(args.refresh_interval_secs));

    let app = build_router(state);
    let listener = tokio::net::TcpListener::bind(&args.listen).await?;
    tracing::info!(
        "seatunnel-web listening on http://{} (masters: {}, auth: {})",
        args.listen,
        args.master,
        if args.auth_disable { "disabled" } else { "enabled" }
    );
    axum::serve(listener, app).await?;
    Ok(())
}
