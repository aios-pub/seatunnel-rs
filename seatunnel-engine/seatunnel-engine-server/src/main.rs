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
        "master" => run_master(&args.addr, engine_config).await?,
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

async fn run_master(
    addr: &str,
    config: seatunnel_engine_server::server_config::EngineServerConfig,
) -> anyhow::Result<()> {
    // Port resolution: CLI --addr > hazelcast port (bind all interfaces)
    // > default 5800.
    let bind_addr = if addr != "0.0.0.0:5800" {
        addr.to_string()
    } else if let Some(port) = config.hazelcast_port {
        format!("0.0.0.0:{}", port)
    } else {
        addr.to_string()
    };

    let coordinator = Arc::new(JobCoordinator::new());
    let registry = new_worker_registry();
    let master_handler = MasterHandler::new(coordinator.clone(), registry.clone());
    let client_handler = ClientHandler::new(coordinator.clone(), registry.clone());
    let replication_handler =
        seatunnel_engine_server::master::ReplicationHandler::new(coordinator.clone());

    let listener = TcpListener::bind(&bind_addr).await?;
    let local_addr = listener.local_addr()?;
    tracing::info!(
        "Master listening on {} (cluster='{}', members={:?})",
        local_addr,
        config.cluster_name,
        config.member_list
    );

    let shutdown = tokio_util::sync::CancellationToken::new();
    let shutdown_signal = shutdown.clone();
    tokio::spawn(async move {
        let _ = tokio::signal::ctrl_c().await;
        shutdown_signal.cancel();
    });

    // Worker-eviction loop: TTL-stale workers are removed and their tasks
    // become claimable by live workers (failover).
    {
        let registry = Arc::clone(&registry);
        let coordinator = Arc::clone(&coordinator);
        let timeout_ms = config.worker_timeout_ms.max(1000);
        let cancel = shutdown.clone();
        tokio::spawn(async move {
            let mut ticker = tokio::time::interval(Duration::from_millis(2000));
            loop {
                tokio::select! {
                    _ = cancel.cancelled() => break,
                    _ = ticker.tick() => {
                        let now = seatunnel_engine_core::now_millis();
                        let stale: Vec<String> = {
                            let reg = registry.read().unwrap();
                            reg.iter()
                                .filter(|(_, e)| now - e.last_heartbeat_ms > timeout_ms as i64)
                                .map(|(id, _)| id.clone())
                                .collect()
                        };
                        for worker_id in stale {
                            registry.write().unwrap().remove(&worker_id);
                            let affected = coordinator.evict_worker(&worker_id);
                            if !affected.is_empty() {
                                tracing::warn!(
                                    "Worker {} evicted (heartbeat timeout > {}ms); {} task(s) reclaimable",
                                    worker_id,
                                    timeout_ms,
                                    affected.len()
                                );
                            } else {
                                tracing::warn!(
                                    "Worker {} evicted (heartbeat timeout > {}ms)",
                                    worker_id,
                                    timeout_ms
                                );
                            }
                        }
                    }
                }
            }
        });
    }

    // HA: standby sync — pull coordinator state from earlier members in
    // the ordered member list. The first member (primary) pulls from
    // nobody; a restarted primary recovers once from any later member
    // that still has state.
    {
        let coordinator = Arc::clone(&coordinator);
        let my_addr = format!("127.0.0.1:{}", local_addr.port());
        let members = config.member_list.clone();
        let interval_ms = config.replication_interval_ms.max(500);
        let cancel = shutdown.clone();
        tokio::spawn(async move {
            // Members earlier than us (we are a standby for them).
            let mut earlier: Vec<String> = Vec::new();
            let mut my_pos = members.len();
            for (idx, member) in members.iter().enumerate() {
                if member == &my_addr || member.ends_with(&format!(":{}", local_addr_port(&my_addr))) {
                    my_pos = idx;
                    break;
                }
            }
            for member in members.iter().take(my_pos) {
                earlier.push(member.clone());
            }
            if earlier.is_empty() {
                tracing::info!("HA: primary master (no earlier member to sync from)");
            } else {
                tracing::info!("HA: standby — syncing from {:?}", earlier);
            }
            let mut ticker = tokio::time::interval(Duration::from_millis(interval_ms));
            let mut recovered_once = earlier.is_empty();
            loop {
                tokio::select! {
                    _ = cancel.cancelled() => break,
                    _ = ticker.tick() => {
                        if earlier.is_empty() && recovered_once {
                            continue; // primary: nothing to pull
                        }
                        let mut pulled = false;
                        for member in &earlier {
                            match pull_state_from(member).await {
                                Ok(Some(snapshot_json)) => {
                                    if let Ok(value) = serde_json::from_str::<serde_json::Value>(&snapshot_json) {
                                        coordinator.import_state(&value).await;
                                        pulled = true;
                                    }
                                    break;
                                }
                                Ok(None) => continue,
                                Err(_) => continue,
                            }
                        }
                        // Restarted primary: one-shot recovery from any
                        // later member that still holds state.
                        if !pulled && earlier.is_empty() && !recovered_once {
                            for member in members.iter().skip(my_pos + 1) {
                                if let Ok(Some(snapshot_json)) = pull_state_from(member).await {
                                    if let Ok(value) = serde_json::from_str::<serde_json::Value>(&snapshot_json) {
                                        coordinator.import_state(&value).await;
                                        tracing::info!("HA: recovered state from {}", member);
                                    }
                                    break;
                                }
                            }
                            recovered_once = true;
                        }
                    }
                }
            }
        });
    }

    // S3 checkpoint sweep (storage type = s3): TTL cleanup of job
    // prefixes, plus terminal-job deletion after the grace window.
    if config.storage_type == "s3" && !config.s3.bucket.is_empty() {
        match seatunnel_engine_server::checkpoint_store::build_object_store(&config.s3) {
            Ok(store) => {
                let s3 = seatunnel_engine_server::checkpoint_store::S3CheckpointStore::new(
                    store,
                    &config.s3.prefix,
                    config.keep_checkpoint_count,
                );
                let ttl = Duration::from_secs(config.history_job_expire_minutes * 60);
                let sweep_every = Duration::from_secs(config.clean_interval_minutes * 60);
                let cancel = shutdown.clone();
                tokio::spawn(async move {
                    let mut ticker = interval(sweep_every);
                    loop {
                        tokio::select! {
                            _ = cancel.cancelled() => break,
                            _ = ticker.tick() => {
                                s3.sweep_expired(ttl).await;
                            }
                        }
                    }
                });
                tracing::info!(
                    "S3 checkpoint sweep active (bucket={}, prefix={}, ttl={}min)",
                    config.s3.bucket,
                    config.s3.prefix,
                    config.history_job_expire_minutes
                );
            }
            Err(e) => tracing::warn!("S3 checkpoint store disabled: {}", e),
        }
    }

    // Hand the already-bound listener to tonic — no re-bind, no race.
    use tokio_stream::wrappers::TcpListenerStream;
    tonic::transport::Server::builder()
        .add_service(MasterServiceServer::new(master_handler))
        .add_service(ClientServiceServer::new(client_handler))
        .add_service(
            seatunnel_engine_comm::ReplicationServiceServer::new(replication_handler),
        )
        .serve_with_incoming(TcpListenerStream::new(listener))
        .await?;

    Ok(())
}

fn local_addr_port(addr: &str) -> u16 {
    addr.rsplit_once(':')
        .and_then(|(_, p)| p.parse().ok())
        .unwrap_or(5800)
}

async fn pull_state_from(member: &str) -> anyhow::Result<Option<String>> {
    use seatunnel_engine_comm::ReplicationServiceClient;
    let url = format!("http://{}", member);
    let mut client = match ReplicationServiceClient::connect(url).await {
        Ok(c) => c,
        Err(_) => return Ok(None),
    };
    let request = tonic::Request::new(seatunnel_engine_comm::PullStateRequest {
        requester_id: "standby".to_string(),
    });
    match client.pull_state(request).await {
        Ok(resp) => {
            let snapshot = resp.into_inner();
            if snapshot.state_json.is_empty() {
                Ok(None)
            } else {
                Ok(Some(snapshot.state_json))
            }
        }
        Err(_) => Ok(None),
    }
}

async fn run_worker(
    addr_arg: &str,
    master_addr: &Option<String>,
    worker_id: &str,
    engine_config: seatunnel_engine_server::server_config::EngineServerConfig,
) -> anyhow::Result<()> {
    // Master list: --master (comma separated) > config member-list >
    // default. Ordered = failover priority.
    let master_list: Vec<String> = master_addr
        .as_ref()
        .map(|m| {
            m.split(',')
                .map(|x| x.trim().to_string())
                .filter(|x| !x.is_empty())
                .collect()
        })
        .filter(|list: &Vec<String>| !list.is_empty())
        .unwrap_or_else(|| engine_config.member_list.clone());
    let master = master_list
        .first()
        .cloned()
        .ok_or_else(|| anyhow::anyhow!("no master address (use --master or the config member-list)"))?;

    // Worker advertise address: --addr > config worker.address > default.
    let addr = if addr_arg != "127.0.0.1:5001" {
        addr_arg.to_string()
    } else {
        engine_config.worker_address.clone()
    };

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
    let mut worker = WorkerNode::new_with_clean(
        worker_id.to_string(),
        addr.clone(),
        state_store,
        clean,
    );
    // Checkpoint storage backend (master/s3 failover support).
    if engine_config.storage_type == "s3" && !engine_config.s3.bucket.is_empty() {
        match seatunnel_engine_server::checkpoint_store::build_object_store(&engine_config.s3) {
            Ok(store) => {
                let s3 = seatunnel_engine_server::checkpoint_store::S3CheckpointStore::new(
                    store,
                    &engine_config.s3.prefix,
                    engine_config.keep_checkpoint_count,
                );
                worker.with_checkpoint_storage("s3", Some(s3));
                tracing::info!(
                    "Worker checkpoint storage: s3 (bucket={}, prefix={})",
                    engine_config.s3.bucket,
                    engine_config.s3.prefix
                );
            }
            Err(e) => {
                tracing::warn!("S3 checkpoint store disabled ({}); using localfile", e);
            }
        }
    } else if engine_config.storage_type == "master" {
        worker.with_checkpoint_storage("master", None);
        tracing::info!("Worker checkpoint storage: master (shared store)");
    }
    let worker = Arc::new(worker);

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

    // Register with the master (reporting running tasks so reassigned
    // ones get fenced via heartbeat preemption on reconnect).
    loop {
        let running = worker.running_task_ids().await;
        let reg_request = tonic::Request::new(WorkerRegistration {
            worker_id: worker_id.to_string(),
            address: addr.to_string(),
            version: env!("CARGO_PKG_VERSION").to_string(),
            resources: Default::default(),
            heartbeat_interval_ms: 2000,
            running_task_ids: running,
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

    let masters_for_hb = master_list.clone();
    let heartbeat_task = tokio::spawn(async move {
        let mut failures: u32 = 0;
        let mut master_idx: usize = 0;
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
                    failures = 0;
                    let response = resp.into_inner();
                    if !response.cancel_jobs.is_empty() {
                        worker_for_hb.cancel_jobs(&response.cancel_jobs).await;
                    }
                    if !response.preempted_task_ids.is_empty() {
                        worker_for_hb
                            .preempt_tasks(&response.preempted_task_ids)
                            .await;
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
                    failures += 1;
                    tracing::warn!("Heartbeat failed ({}/3): {}", failures, e);
                    if failures >= 3 && masters_for_hb.len() > 1 {
                        master_idx = (master_idx + 1) % masters_for_hb.len();
                        let next = masters_for_hb[master_idx].clone();
                        tracing::error!(
                            "Master unreachable 3x — failing over to {} (data plane continues)",
                            next
                        );
                        match MasterServiceClient::connect(format!("http://{}", next)).await {
                            Ok(mut new_client) => {
                                // Re-register with running tasks so the new
                                // master fences reassigned tasks properly.
                                let running = worker_for_hb.running_task_ids().await;
                                let reg = tonic::Request::new(WorkerRegistration {
                                    worker_id: worker_id_owned.clone(),
                                    address: addr_owned.clone(),
                                    version: env!("CARGO_PKG_VERSION").to_string(),
                                    resources: Default::default(),
                                    heartbeat_interval_ms: 2000,
                                    running_task_ids: running,
                                });
                                if let Err(err) = new_client.register_worker(reg).await {
                                    tracing::warn!(
                                        "Re-registration with {} failed: {}",
                                        next,
                                        err
                                    );
                                }
                                worker_for_hb.set_master_client(new_client.clone()).await;
                                client = new_client;
                                failures = 0;
                                tracing::info!("Failed over to master {}", next);
                            }
                            Err(err) => {
                                tracing::warn!("Failover target {} unreachable too: {}", next, err);
                                failures = 0; // rotate again next round
                            }
                        }
                    }
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
