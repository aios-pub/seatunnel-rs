/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.
 */

use anyhow::Result;
use clap::Parser as _;
use seatunnel_cli::Cli;

#[tokio::main]
async fn main() -> Result<()> {
    // Parse first so --debug / --log-level can shape the log filter.
    let cli = Cli::parse();

    // Same format as the engine servers: local time YYYY-MM-DD HH:mm:ss.
    use tracing_subscriber::{EnvFilter, fmt::Layer, prelude::*};
    // Precedence: --log-level > --debug > RUST_LOG > "info".
    let filter = match cli.log_level.as_deref() {
        Some(level) => EnvFilter::try_new(level).unwrap_or_else(|_| {
            eprintln!("warning: invalid --log-level '{level}', falling back to RUST_LOG/info");
            EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into())
        }),
        None if cli.debug => EnvFilter::new("debug"),
        None => EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()),
    };
    tracing_subscriber::registry()
        .with(filter)
        .with(
            Layer::default().with_timer(tracing_subscriber::fmt::time::ChronoLocal::new(
                "%Y-%m-%d %H:%M:%S".to_string(),
            )),
        )
        .init();
    seatunnel_cli::execute(cli).await?;
    Ok(())
}
