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
//!   seatunnel-engine-server --role master  --addr 0.0.0.0:5800
//!   seatunnel-engine-server --role worker  --master <master-address> [--worker-id w1]
//!   seatunnel-engine-server --role hybrid  --addr 0.0.0.0:5800
//!
//! `hybrid` runs the coordinator and a worker executor in one process —
//! the recommended single-machine form (Java `MASTER_AND_WORKER` parity)
//! and the building block of symmetric multi-node deployments.

use clap::Parser;
use seatunnel_engine_comm::{
    HeartbeatRequest, WorkerRegistration, generated::master_service_client::MasterServiceClient,
};
use seatunnel_engine_server::{
    ClientHandler, JobCoordinator, LocalStateStore, MasterHandler, WorkerNode, new_worker_registry,
    server_config::EngineServerConfig,
};
use std::sync::Arc;
use tokio::net::TcpListener;
use tokio::time::Duration;
use tokio_util::sync::CancellationToken;
use tracing_subscriber::{EnvFilter, fmt::Layer, prelude::*};

use seatunnel_engine_comm::generated::client_service_server::ClientServiceServer;
use seatunnel_engine_comm::generated::master_service_server::MasterServiceServer;
use seatunnel_engine_comm::generated::raft_service_server::RaftServiceServer;
use seatunnel_engine_server::master::MasterInfo;
use seatunnel_engine_server::raft::{
    LeaderState, LeaderView, RaftWrite, WritePath, members_from_addresses, spawn_leader_watcher,
    start_node, validate_voters,
};

#[derive(Parser, Debug)]
#[command(
    name = "seatunnel-engine-server",
    about = "SeaTunnel Engine Server (Master/Worker/Hybrid)"
)]
struct Args {
    #[arg(long, default_value = "master")]
    role: String,

    #[arg(long, default_value = "0.0.0.0:5800")]
    addr: String,

    /// Address other nodes should use to reach this master (host:port).
    /// Defaults to the --addr host (or 127.0.0.1 when binding a
    /// wildcard) plus the bound port.
    #[arg(long)]
    advertise_addr: Option<String>,

    /// Master address(es), comma separated (required for workers).
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
    let engine_config = EngineServerConfig::load(
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
        "master" => run_master(&args.addr, &args.advertise_addr, engine_config).await?,
        "worker" => {
            let master_list = resolve_master_list(&args.master, &engine_config)?;
            run_worker(&args.addr, master_list, &args.worker_id, engine_config).await?;
        }
        "hybrid" => {
            run_hybrid(&args.addr, &args.advertise_addr, &args.worker_id, engine_config).await?
        }
        other => {
            eprintln!(
                "Unknown role: {}. Use 'master', 'worker' or 'hybrid'.",
                other
            );
            std::process::exit(1);
        }
    }

    Ok(())
}

/// Master list: --master (comma separated) > config member-list > default.
/// Ordered = failover priority.
fn resolve_master_list(
    master_arg: &Option<String>,
    engine_config: &EngineServerConfig,
) -> anyhow::Result<Vec<String>> {
    let list: Vec<String> = master_arg
        .as_ref()
        .map(|m| {
            m.split(',')
                .map(|x| x.trim().to_string())
                .filter(|x| !x.is_empty())
                .collect()
        })
        .filter(|list: &Vec<String>| !list.is_empty())
        .unwrap_or_else(|| engine_config.member_list.clone());
    if list.is_empty() {
        anyhow::bail!("no master address (use --master or the config member-list)");
    }
    Ok(list)
}

/// The address other nodes should use to reach a master bound at
/// `bind_addr`: --advertise-addr > the bind host (if routable) >
/// loopback, always with the actually bound port.
fn advertise_address(advertise: &Option<String>, local_addr: &std::net::SocketAddr) -> String {
    if let Some(explicit) = advertise.as_deref().filter(|a| !a.is_empty()) {
        return explicit.to_string();
    }
    let host = local_addr.ip().to_string();
    let host = if host == "0.0.0.0" || host == "::" {
        "127.0.0.1".to_string()
    } else {
        host
    };
    format!("{}:{}", host, local_addr.port())
}

/// Everything the gRPC stack needs that the rest of main also touches.
struct MasterServing {
    shutdown: CancellationToken,
    local_addr: std::net::SocketAddr,
}

/// Resolve this node's raft voter id: the position of its advertise
/// address (or matching port) in the member list.
fn my_voter_id(advertise: &str, members: &[String]) -> Option<u64> {
    let my_port = advertise.rsplit_once(':').and_then(|(_, p)| p.parse::<u16>().ok());
    for (idx, member) in members.iter().enumerate() {
        if member == advertise {
            return Some(idx as u64 + 1);
        }
        if let (Some(port), Some(mport)) = (my_port, member.rsplit_once(':').and_then(|(_, p)| p.parse::<u16>().ok())) {
            if port == mport && member.starts_with("127.0.0.1") {
                return Some(idx as u64 + 1);
            }
        }
    }
    None
}

/// Bind the listener and spawn the master-side loops. HA is openraft:
/// the member list doubles as the voter set (validated odd), the leader
/// term feeds the wire fencing, and writes go through the Raft log.
async fn start_master(
    bind_addr: &str,
    advertise: &Option<String>,
    config: EngineServerConfig,
    role: &str,
) -> anyhow::Result<(MasterServing, impl std::future::Future<Output = ()> + Send)> {
    let coordinator = Arc::new(JobCoordinator::new());
    let registry = new_worker_registry();
    let listener = TcpListener::bind(bind_addr).await?;
    let local_addr = listener.local_addr()?;
    let advertise_addr = advertise_address(advertise, &local_addr);
    let info = MasterInfo {
        advertise_addr: advertise_addr.clone(),
        role: role.to_string(),
    };

    // --- Consensus bootstrap -------------------------------------------
    validate_voters(config.member_list.len())?;
    let node_id = my_voter_id(&advertise_addr, &config.member_list).ok_or_else(|| {
        anyhow::anyhow!(
            "advertise address {} not found in the member list {:?} — \
             every master/hybrid node must list itself",
            advertise_addr,
            config.member_list
        )
    })?;
    let members = members_from_addresses(&config.member_list);
    let raft_dir = std::path::Path::new(&config.state_dir).join("raft");
    let raft = (*start_node(node_id, members.clone(), Arc::clone(&coordinator), raft_dir).await?).clone();
    let leader: LeaderView = Arc::new(std::sync::RwLock::new(LeaderState::default()));
    spawn_leader_watcher(
        raft.clone(),
        node_id,
        Arc::clone(&leader),
        Arc::clone(&coordinator),
    );
    let writes: Arc<dyn WritePath> =
        Arc::new(RaftWrite::new(raft.clone(), node_id, members.clone(), Arc::clone(&leader)));
    tracing::info!(
        "Master({}) listening on {} advertise={} raft-id={} voters={} (cluster='{}')",
        role,
        local_addr,
        advertise_addr,
        node_id,
        config.member_list.len(),
        config.cluster_name
    );

    let master_handler = MasterHandler::new(
        coordinator.clone(),
        registry.clone(),
        info.clone(),
        config.heartbeat_interval_ms,
        config.worker_soft_timeout_ms,
        writes.clone(),
    )
    .with_dispatch_batch_limit(config.dispatch_batch_limit);
    let client_handler = ClientHandler::new(
        coordinator.clone(),
        registry.clone(),
        info,
        writes.clone(),
        master_handler.wake_signal(),
    );
    let raft_handler = seatunnel_engine_server::raft::network::RaftServiceHandler {
        raft: raft.clone(),
    };

    let shutdown = CancellationToken::new();
    let shutdown_signal = shutdown.clone();
    tokio::spawn(async move {
        let _ = tokio::signal::ctrl_c().await;
        shutdown_signal.cancel();
    });

    // Worker-eviction loop: hard-TTL-stale workers are removed and their
    // tasks become claimable by live workers (failover). The same tick
    // aborts coordinated checkpoints whose prepares never arrived.
    {
        let registry = Arc::clone(&registry);
        let coordinator = Arc::clone(&coordinator);
        let writes = writes.clone();
        let wake = master_handler.wake_signal();
        let timeout_ms = config.worker_timeout_ms.max(config.worker_soft_timeout_ms).max(1000);
        let cp_timeout_ms = config.checkpoint_timeout_ms.max(1000);
        let cancel = shutdown.clone();
        tokio::spawn(async move {
            let mut ticker = tokio::time::interval(Duration::from_millis(2000));
            loop {
                tokio::select! {
                    _ = cancel.cancelled() => break,
                    _ = ticker.tick() => {
                        let aborted = coordinator.abort_timed_out_checkpoints(cp_timeout_ms);
                        if aborted > 0 {
                            tracing::warn!(
                                "coordinated checkpoints aborted (timeout {}ms): {}",
                                cp_timeout_ms,
                                aborted
                            );
                        }
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
                            // Durable release of the dead worker's tasks.
                            let cmd = seatunnel_engine_server::job_coordinator::Command::EvictWorker {
                                worker_id: worker_id.clone(),
                            };
                            if let Err(e) = writes.propose(cmd).await {
                                tracing::warn!("EvictWorker proposal failed: {}", e);
                            }
                            // Released tasks are claimable — wake parked
                            // long-poll heartbeats.
                            wake.notify_waiters();
                            tracing::warn!(
                                "Worker {} evicted (heartbeat timeout > {}ms)",
                                worker_id,
                                timeout_ms
                            );
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
                    let mut ticker = tokio::time::interval(sweep_every);
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
    let server = async move {
        let _ = tonic::transport::Server::builder()
            .add_service(MasterServiceServer::new(master_handler))
            .add_service(ClientServiceServer::new(client_handler))
            .add_service(RaftServiceServer::new(raft_handler))
            .serve_with_incoming(TcpListenerStream::new(listener))
            .await;
    };

    Ok((
        MasterServing {
            shutdown,
            local_addr,
        },
        server,
    ))
}

async fn run_master(
    addr: &str,
    advertise: &Option<String>,
    config: EngineServerConfig,
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

    let (serving, server) = start_master(&bind_addr, advertise, config, "master").await?;
    let shutdown = serving.shutdown;
    tokio::select! {
        _ = server => {},
        _ = shutdown.cancelled() => {
            tracing::info!("Master shutting down.");
        }
    }
    Ok(())
}

/// Hybrid role: one process = full cluster. The coordinator serves gRPC
/// and a local worker executor heartbeats it over the loopback.
async fn run_hybrid(
    addr: &str,
    advertise: &Option<String>,
    worker_id: &str,
    config: EngineServerConfig,
) -> anyhow::Result<()> {
    let bind_addr = if addr != "0.0.0.0:5800" {
        addr.to_string()
    } else if let Some(port) = config.hazelcast_port {
        format!("0.0.0.0:{}", port)
    } else {
        addr.to_string()
    };

    let (serving, server) = start_master(&bind_addr, advertise, config.clone(), "hybrid").await?;
    let advertise_addr = advertise_address(advertise, &serving.local_addr);
    tracing::info!(
        "Hybrid node: coordinator at {} + in-process worker '{}'",
        advertise_addr,
        worker_id
    );

    // The in-process worker talks to this node's own coordinator.
    let master_list = vec![advertise_addr];
    let worker_bind_addr = format!("127.0.0.1:{}", serving.local_addr.port());
    let worker_fut = run_worker(&worker_bind_addr, master_list, worker_id, config);

    tokio::select! {
        _ = server => {},
        res = worker_fut => {
            if let Err(e) = res {
                tracing::error!("Hybrid worker executor failed: {}", e);
            }
        }
    }
    Ok(())
}

async fn run_worker(
    addr_arg: &str,
    master_list: Vec<String>,
    worker_id: &str,
    engine_config: EngineServerConfig,
) -> anyhow::Result<()> {
    let master = master_list.first().cloned().ok_or_else(|| {
        anyhow::anyhow!("no master address (use --master or the config member-list)")
    })?;

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
    let clean = engine_config
        .auto_clean
        .then(|| seatunnel_engine_server::worker::CleanConfig {
            grace_secs: engine_config.clean_grace_minutes * 60,
            interval_secs: engine_config.clean_interval_minutes * 60,
            ttl_secs: engine_config.history_job_expire_minutes * 60,
        });
    let mut worker =
        WorkerNode::new_with_clean(worker_id.to_string(), addr.clone(), state_store, clean);
    // Dynamic admission: measured pressure (lag + memory watermark), no
    // slot counts. Samplers run inside the controller.
    worker = worker.with_admission(seatunnel_engine_server::admission::AdmissionController::new(
        engine_config.admission_config(),
    ));
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
        "Worker '{}' starting at {} → master {} (state dir: {}, retained={}, auto-clean={}, \\
         admission: lag<{}ms && mem<{}%, cooldown {}s)",
        worker_id,
        addr,
        master,
        state_dir,
        engine_config.keep_checkpoint_count,
        engine_config.auto_clean,
        engine_config.overload_lag_ms,
        engine_config.memory_watermark_percent,
        engine_config.overload_cooldown_secs
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
            heartbeat_interval_ms: engine_config.heartbeat_interval_ms as i64,
            running_task_ids: running,
            slots: 0, // deprecated
        });
        match client.register_worker(reg_request).await {
            Ok(resp) => {
                let resp = resp.into_inner();
                worker.observe_term(resp.term);
                tracing::info!(
                    "Registered with master (term={}, leader={})",
                    resp.term,
                    if resp.leader_address.is_empty() {
                        "-"
                    } else {
                        &resp.leader_address
                    }
                );
                break;
            }
            Err(e) => {
                tracing::warn!("Registration failed ({}); retrying in 2s", e);
                tokio::time::sleep(Duration::from_secs(2)).await;
            }
        }
    }

    // Heartbeat loop: report liveness + running tasks, pull new assignments
    // and cancellation notices. The interval adapts to the master's
    // `next_interval_ms` (clamped to sane bounds).
    let shutdown = CancellationToken::new();
    let shutdown_signal = shutdown.clone();
    let hb_handle = tokio::spawn({
        let shutdown_signal = shutdown_signal;
        let worker = Arc::clone(&worker);
        let worker_id = worker_id.to_string();
        let addr = addr.clone();
        let masters = master_list.clone();
        let default_interval = engine_config.heartbeat_interval_ms;
        let slots = 0u32; // deprecated field, kept for wire compatibility
        async move {
            let mut failures: u32 = 0;
            let mut master_idx: usize = 0;
            let mut interval_ms = default_interval;
            loop {
                tokio::select! {
                    _ = shutdown_signal.cancelled() => break,
                    _ = tokio::time::sleep(Duration::from_millis(
                        // With long-polling the SERVER paces the cadence
                        // (parking up to `interval` server-side); the
                        // client-side gap only catches transport errors.
                        100.min(interval_ms)
                    )) => {}
                }

                let tasks = worker.heartbeat_tasks().await;
                let (load_score, lag_ms, mem_permille, can_accept) =
                    worker.admission_fields().await;
                let hb = HeartbeatRequest {
                    worker_id: worker_id.clone(),
                    address: addr.clone(),
                    timestamp: seatunnel_engine_core::now_millis(),
                    tasks,
                    term: worker.term(),
                    // Long-poll: the master parks this request until
                    // dispatchable work appears (or the budget expires),
                    // so task handout latency drops from a full heartbeat
                    // interval to ~0 while the connection stays
                    // worker-initiated (NAT-friendly).
                    wait_ms: interval_ms as i64,
                    // Dynamic admission signals (measured pressure).
                    load_score,
                    lag_ms,
                    mem_permille,
                    can_accept,
                };

                match client.heartbeat(hb).await {
                    Ok(resp) => {
                        failures = 0;
                        let response = resp.into_inner();
                        interval_ms = if response.next_interval_ms > 0 {
                            (response.next_interval_ms as u64).clamp(250, 60_000)
                        } else {
                            default_interval
                        };
                        // Term-fenced application: a deposed master's
                        // instructions are rejected inside.
                        worker.apply_master_response(&response).await;
                    }
                    Err(e) => {
                        failures += 1;
                        tracing::warn!("Heartbeat failed ({}/3): {}", failures, e);
                        if failures >= 3 && masters.len() > 1 {
                            master_idx = (master_idx + 1) % masters.len();
                            let next = masters[master_idx].clone();
                            tracing::error!(
                                "Master unreachable 3x — failing over to {} (data plane continues)",
                                next
                            );
                            match MasterServiceClient::connect(format!("http://{}", next)).await {
                                Ok(mut new_client) => {
                                    // Re-register with running tasks so the new
                                    // master fences reassigned tasks properly.
                                    let running = worker.running_task_ids().await;
                                    let reg = tonic::Request::new(WorkerRegistration {
                                        worker_id: worker_id.clone(),
                                        address: addr.clone(),
                                        version: env!("CARGO_PKG_VERSION").to_string(),
                                        resources: Default::default(),
                                        heartbeat_interval_ms: default_interval as i64,
                                        running_task_ids: running,
                                        slots,
                                    });
                                    match new_client.register_worker(reg).await {
                                        Ok(resp) => {
                                            let resp = resp.into_inner();
                                            worker.observe_term(resp.term);
                                        }
                                        Err(err) => {
                                            tracing::warn!(
                                                "Re-registration with {} failed: {}",
                                                next,
                                                err
                                            );
                                        }
                                    }
                                    worker.set_master_client(new_client.clone()).await;
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
        }
    });

    // Wait for shutdown, then leave the cluster cleanly: unregistering
    // releases this worker's tasks for takeover immediately instead of
    // making peers wait out the hard eviction timeout.
    tokio::select! {
        _ = hb_handle => {},
        _ = tokio::signal::ctrl_c() => {
            tracing::info!("Worker '{}' shutting down.", worker_id);
            shutdown.cancel();
        }
    }
    let _ = tokio::time::timeout(Duration::from_secs(2), async {
        if let Ok(mut c) = MasterServiceClient::connect(format!("http://{}", master)).await {
            let req = tonic::Request::new(seatunnel_engine_comm::UnregisterWorkerRequest {
                worker_id: worker_id.to_string(),
                address: addr.clone(),
            });
            if let Err(e) = c.unregister_worker(req).await {
                tracing::warn!("Graceful unregister failed: {}", e);
            }
        }
    })
    .await;

    Ok(())
}
