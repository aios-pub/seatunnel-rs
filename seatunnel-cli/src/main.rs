/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.
 */

use anyhow::Result;
use clap::Parser as _;
use seatunnel_cli::Cli;

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();
    seatunnel_cli::execute(cli).await?;
    Ok(())
}
