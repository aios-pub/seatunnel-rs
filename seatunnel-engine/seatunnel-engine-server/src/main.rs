/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

//! SeaTunnel Engine Server — gRPC Master + Worker node.
//!
//! Usage:
//!   seatunnel-engine-server --role master --addr 0.0.0.0:5000
//!   seatunnel-engine-server --role worker --master <master-address>

use clap::Parser;
use seatunnel_engine_server::{ClientHandler, JobManager, MasterHandler};
use std::sync::Arc;
use tokio::net::TcpListener;
use tracing_subscriber::{fmt::Layer, prelude::*, EnvFilter};

use seatunnel_engine_comm::generated::master_service_server::MasterServiceServer;
use seatunnel_engine_comm::generated::client_service_server::ClientServiceServer;

mod client_handler;
mod job_manager;
mod leader_election;
mod master;
mod resource_manager;
mod worker;

#[derive(Parser, Debug)]
#[command(name = "seatunnel-engine-server", about = "SeaTunnel Engine Server (Master/Worker)")]
struct Args {
    #[arg(long, default_value = "master")]
    role: String,

    #[arg(long, default_value = "0.0.0.0:5000")]
    addr: String,

    #[arg(long)]
    master: Option<String>,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::registry()
        .with(EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()))
        .with(Layer::default())
        .init();

    let args = Args::parse();

    match args.role.as_str() {
        "master" => run_master(&args.addr).await?,
        "worker" => run_worker(&args.addr, &args.master).await?,
        other => {
            eprintln!("Unknown role: {}. Use 'master' or 'worker'.", other);
            std::process::exit(1);
        }
    }

    Ok(())
}

async fn run_master(addr: &str) -> anyhow::Result<()> {
    let listener = TcpListener::bind(addr).await?;
    let job_manager = Arc::new(JobManager::new());
    let master_handler = MasterHandler::new();
    let client_handler = ClientHandler::new(job_manager);

    tracing::info!("Starting master on {}", addr);

    let shutdown = tokio_util::sync::CancellationToken::new();
    let shutdown_signal = shutdown.clone();

    // Spawn shutdown on Ctrl+C
    tokio::spawn(async move {
        let _ = tokio::signal::ctrl_c().await;
        shutdown_signal.cancel();
    });

    let local_addr = listener.local_addr()?;
    // Spawn the server on the bound address
    let server = tonic::transport::Server::builder()
        .add_service(MasterServiceServer::new(master_handler))
        .add_service(ClientServiceServer::new(client_handler));

    // Drop the original listener, let the server bind to local_addr
    drop(listener);
    server
        .serve_with_shutdown(local_addr, shutdown.cancelled())
        .await?;

    Ok(())
}

async fn run_worker(addr: &str, master_addr: &Option<String>) -> anyhow::Result<()> {
    let master = master_addr
        .as_ref()
        .expect("--master is required for worker role");
    tracing::info!(
        "Starting worker on {}, connecting to master at {}",
        addr,
        master
    );

    println!("Worker ready at {}. Connect to master at {}.", addr, master);
    println!("Worker will register and wait for task assignments.");

    // Block until shutdown
    tokio::signal::ctrl_c().await?;
    println!("Worker shutting down.");

    Ok(())
}
