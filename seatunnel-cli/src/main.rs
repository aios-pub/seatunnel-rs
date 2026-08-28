/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.
 */

use anyhow::Result;
use clap::Parser as _;
use seatunnel_cli::Cli;

#[tokio::main]
async fn main() -> Result<()> {
    // Same format as the engine servers: local time YYYY-MM-DD HH:mm:ss.
    use tracing_subscriber::{EnvFilter, fmt::Layer, prelude::*};
    tracing_subscriber::registry()
        .with(EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()))
        .with(
            Layer::default().with_timer(tracing_subscriber::fmt::time::ChronoLocal::new(
                "%Y-%m-%d %H:%M:%S".to_string(),
            )),
        )
        .init();
    let cli = Cli::parse();
    seatunnel_cli::execute(cli).await?;
    Ok(())
}
