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
//!   seatunnel-engine-server --role master --addr 0.0.0.0:5800
//!   seatunnel-engine-server --role worker --master <master-address> [--worker-id w1]

use clap::Parser;
use seatunnel_engine_comm::{
    generated::master_service_client::MasterServiceClient, HeartbeatRequest, WorkerRegistration,
};
use seatunnel_engine_server::{
    new_worker_registry, ClientHandler, JobCoordinator, LocalStateStore, MasterHandler, WorkerNode,
};
use std::sync::Arc;
use tokio::net::TcpListener;
use tokio::time::{interval, Duration};
use tracing_subscriber::{fmt::Layer, prelude::*, EnvFilter};

use seatunnel_engine_comm::generated::client_service_server::ClientServiceServer;
use seatunnel_engine_comm::generated::master_service_server::MasterServiceServer;

#[derive(Parser, Debug)]
#[command(
    name = "seatunnel-engine-server",
    about = "SeaTunnel Engine Server (Master/Worker)"
)]
struct Args {
    #[arg(long, default_value = "master")]
    role: String,

    #[arg(long, default_value = "0.0.0.0:5800")]
    addr: String,

    /// Master address (required for workers).
    #[arg(long)]
    master: Option<String>,

    #[arg(long, default_value = "worker-1")]
    worker_id: String,

    /// Directory for durable checkpoint state (workers).
    #[arg(long, env = "SEATUNNEL_STATE_DIR", default_value = ".seatunnel-state")]
    state_dir: String,

    /// Engine config file (Java `seatunnel.yaml` adapted); see
    /// config/seatunnel.yaml for the reference layout.
    #[arg(long, short = 'f')]
    config: Option<String>,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::registry()
        .with(EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()))
        .with(
            Layer::default().with_timer(tracing_subscriber::fmt::time::ChronoLocal::new(
                // "YYYY-MM-DD HH:mm:ss" in the server's local timezone.
                "%Y-%m-%d %H:%M:%S".to_string(),
            )),
        )
        .init();

    let args = Args::parse();

    // Precedence: --state-dir > SEATUNNEL_STATE_DIR > config file > default.
    let explicit_state_dir = if args.state_dir == ".seatunnel-state" {
        None
    } else {
        Some(args.state_dir.as_str())
    };
    let engine_config = seatunnel_engine_server::server_config::EngineServerConfig::load(
        args.config.as_deref(),
        explicit_state_dir,
        std::env::var("SEATUNNEL_STATE_DIR").ok().as_deref(),
    )?;
    tracing::info!(
        "Engine config: state_dir={} keep-checkpoint-count={} checkpoint-interval={}ms \
         auto-clean={} grace={}min sweep-every={}min ttl={}min",
        engine_config.state_dir,
        engine_config.keep_checkpoint_count,
        engine_config.checkpoint_interval,
        engine_config.auto_clean,
        engine_config.clean_grace_minutes,
        engine_config.clean_interval_minutes,
        engine_config.history_job_expire_minutes
    );

    match args.role.as_str() {
        "master" => run_master(&args.addr).await?,
        "worker" => {
            run_worker(&args.addr, &args.master, &args.worker_id, engine_config).await?
        }
        other => {
            eprintln!("Unknown role: {}. Use 'master' or 'worker'.", other);
            std::process::exit(1);
        }
    }

    Ok(())
}

async fn run_master(addr: &str) -> anyhow::Result<()> {
    let coordinator = Arc::new(JobCoordinator::new());
    let registry = new_worker_registry();
    let master_handler = MasterHandler::new(coordinator.clone(), registry.clone());
    let client_handler = ClientHandler::new(coordinator, registry);

    let listener = TcpListener::bind(addr).await?;
    let local_addr = listener.local_addr()?;
    tracing::info!("Master listening on {}", local_addr);

    let shutdown = tokio_util::sync::CancellationToken::new();
    let shutdown_signal = shutdown.clone();
    tokio::spawn(async move {
        let _ = tokio::signal::ctrl_c().await;
        shutdown_signal.cancel();
    });

    // Hand the already-bound listener to tonic — no re-bind, no race.
    use tokio_stream::wrappers::TcpListenerStream;
    tonic::transport::Server::builder()
        .add_service(MasterServiceServer::new(master_handler))
        .add_service(ClientServiceServer::new(client_handler))
        .serve_with_incoming(TcpListenerStream::new(listener))
        .await?;

    Ok(())
}

async fn run_worker(
    addr: &str,
    master_addr: &Option<String>,
    worker_id: &str,
    engine_config: seatunnel_engine_server::server_config::EngineServerConfig,
) -> anyhow::Result<()> {
    let master = master_addr
        .as_ref()
        .ok_or_else(|| anyhow::anyhow!("--master is required for worker role"))?;

    let state_dir = engine_config.state_dir.clone();
    let state_store = Arc::new(LocalStateStore::with_retention(
        &state_dir,
        engine_config.keep_checkpoint_count,
    ));
    let clean = engine_config.auto_clean.then(|| {
        seatunnel_engine_server::worker::CleanConfig {
            grace_secs: engine_config.clean_grace_minutes * 60,
            interval_secs: engine_config.clean_interval_minutes * 60,
            ttl_secs: engine_config.history_job_expire_minutes * 60,
        }
    });
    let worker = Arc::new(WorkerNode::new_with_clean(
        worker_id.to_string(),
        addr.to_string(),
        state_store,
        clean,
    ));

    tracing::info!(
        "Worker '{}' starting at {} → master {} (state dir: {}, retained={}, auto-clean={})",
        worker_id,
        addr,
        master,
        state_dir,
        engine_config.keep_checkpoint_count,
        engine_config.auto_clean
    );

    // Background state cleaner (TTL sweep; cancel cleanup rides along).
    if let Some(clean) = clean {
        seatunnel_engine_server::worker::spawn_state_cleaner(Arc::clone(&worker), clean);
    }

    // Connect (with retry so the master can be started first).
    let master_url = format!("http://{}", master);
    let mut client = loop {
        match MasterServiceClient::connect(master_url.clone()).await {
            Ok(c) => break c,
            Err(e) => {
                tracing::warn!(
                    "Cannot reach master at {} yet ({}); retrying in 2s",
                    master,
                    e
                );
                tokio::time::sleep(Duration::from_secs(2)).await;
            }
        }
    };

    worker.set_master_client(client.clone()).await;

    // Register with the master.
    loop {
        let reg_request = tonic::Request::new(WorkerRegistration {
            worker_id: worker_id.to_string(),
            address: addr.to_string(),
            version: env!("CARGO_PKG_VERSION").to_string(),
            resources: Default::default(),
            heartbeat_interval_ms: 2000,
        });
        match client.register_worker(reg_request).await {
            Ok(resp) => {
                tracing::info!("Registered with master: {}", resp.into_inner().message);
                break;
            }
            Err(e) => {
                tracing::warn!("Registration failed ({}); retrying in 2s", e);
                tokio::time::sleep(Duration::from_secs(2)).await;
            }
        }
    }

    // Heartbeat loop: report liveness + running tasks, pull new assignments
    // and cancellation notices.
    let mut heartbeat = interval(Duration::from_millis(2000));
    heartbeat.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    let worker_for_hb = Arc::clone(&worker);
    let worker_id_owned = worker_id.to_string();
    let addr_owned = addr.to_string();

    let heartbeat_task = tokio::spawn(async move {
        loop {
            heartbeat.tick().await;

            let tasks = worker_for_hb.heartbeat_tasks().await;
            let hb = HeartbeatRequest {
                worker_id: worker_id_owned.clone(),
                address: addr_owned.clone(),
                timestamp: seatunnel_engine_core::now_millis(),
                tasks,
            };

            match client.heartbeat(hb).await {
                Ok(resp) => {
                    let response = resp.into_inner();
                    if !response.cancel_jobs.is_empty() {
                        worker_for_hb.cancel_jobs(&response.cancel_jobs).await;
                    }
                    if !response.pending_tasks.is_empty() {
                        tracing::info!(
                            "Received {} task(s) from master",
                            response.pending_tasks.len()
                        );
                        for task in response.pending_tasks {
                            worker_for_hb.assign_task(task).await;
                        }
                    }
                }
                Err(e) => {
                    tracing::warn!("Heartbeat failed: {}", e);
                }
            }
        }
    });

    // Wait for shutdown.
    tokio::select! {
        _ = heartbeat_task => {},
        _ = tokio::signal::ctrl_c() => {
            tracing::info!("Worker '{}' shutting down.", worker_id);
        }
    }

    Ok(())
}
