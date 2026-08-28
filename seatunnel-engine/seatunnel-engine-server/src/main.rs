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
//!
//! The web management console is compiled into this binary; pass `--web`
//! to also serve it (see the `--web-*` flags).

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

    /// Enable debug logging (verbose output for the data pipeline).
    #[arg(long, env = "SEATUNNEL_DEBUG")]
    debug: bool,

    /// Log level filter: trace|debug|info|warn|error (takes precedence
    /// over --debug; RUST_LOG is used when neither is given).
    #[arg(long, env = "SEATUNNEL_LOG")]
    log_level: Option<String>,

    /// Serve the embedded web management console (SPA + REST API +
    /// /metrics) from this process.
    #[arg(long)]
    web: bool,

    /// HTTP listen address of the embedded web console (requires --web).
    #[arg(long, default_value = "0.0.0.0:8080")]
    web_listen: String,

    /// Engine endpoint(s) the console proxies to, comma separated
    /// (failover order). Defaults to this server's own gRPC endpoint
    /// (master/hybrid) or the --master list (worker role).
    #[arg(long)]
    web_master: Option<String>,

    /// Web console login username.
    #[arg(long, env = "SEATUNNEL_WEB_USER", default_value = "admin")]
    web_auth_user: String,

    /// Web console login password; falls back to "admin" with a startup
    /// warning when unset.
    #[arg(long, env = "SEATUNNEL_WEB_PASSWORD")]
    web_auth_password: Option<String>,

    /// Disable web console authentication (local development only).
    #[arg(long)]
    web_auth_disable: bool,
}

/// Options of the embedded web console, gathered from the `--web-*` flags.
#[derive(Debug, Clone)]
struct WebConsoleArgs {
    listen: String,
    /// Engine endpoint override; `None` derives it from the node role.
    master: Option<String>,
    auth_user: String,
    auth_password: Option<String>,
    auth_disable: bool,
}

/// Console auth from the `--web-auth-*` flags; mirrors the standalone
/// `seatunnel-web` binary's defaults (admin / "admin" with a warning).
fn web_auth(web: &WebConsoleArgs) -> seatunnel_web::AuthConfig {
    if web.auth_disable {
        tracing::warn!("web console authentication disabled (--web-auth-disable)");
        return seatunnel_web::AuthConfig::disabled();
    }
    let password = web.auth_password.clone().unwrap_or_else(|| {
        tracing::warn!(
            "no web console password (--web-auth-password / SEATUNNEL_WEB_PASSWORD); \
             using the default \"admin\" — change it for anything beyond local use"
        );
        "admin".to_string()
    });
    seatunnel_web::AuthConfig::new(web.auth_user.clone(), password, 12 * 3600)
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Parse first so --debug / --log-level can shape the log filter.
    let args = Args::parse();

    // Precedence: --log-level > --debug > RUST_LOG > "info".
    let filter = match args.log_level.as_deref() {
        Some(level) => EnvFilter::try_new(level).unwrap_or_else(|_| {
            eprintln!("warning: invalid --log-level '{level}', falling back to RUST_LOG/info");
            EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into())
        }),
        None if args.debug => EnvFilter::new("debug"),
        None => EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()),
    };
    tracing_subscriber::registry()
        .with(filter)
        .with(
            Layer::default().with_timer(tracing_subscriber::fmt::time::ChronoLocal::new(
                // "YYYY-MM-DD HH:mm:ss" in the server's local timezone.
                "%Y-%m-%d %H:%M:%S".to_string(),
            )),
        )
        .init();

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

    let web = args.web.then(|| WebConsoleArgs {
        listen: args.web_listen.clone(),
        master: args.web_master.clone(),
        auth_user: args.web_auth_user.clone(),
        auth_password: args.web_auth_password.clone(),
        auth_disable: args.web_auth_disable,
    });

    match args.role.as_str() {
        "master" => run_master(&args.addr, &args.advertise_addr, engine_config, web).await?,
        "worker" => {
            let master_list = resolve_master_list(&args.master, &engine_config)?;
            run_worker(&args.addr, master_list, &args.worker_id, engine_config, web).await?;
        }
        "hybrid" => {
            run_hybrid(
                &args.addr,
                &args.advertise_addr,
                &args.worker_id,
                engine_config,
                web,
            )
            .await?
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
    let my_port = advertise
        .rsplit_once(':')
        .and_then(|(_, p)| p.parse::<u16>().ok());
    for (idx, member) in members.iter().enumerate() {
        if member == advertise {
            return Some(idx as u64 + 1);
        }
        if let (Some(port), Some(mport)) = (
            my_port,
            member
                .rsplit_once(':')
                .and_then(|(_, p)| p.parse::<u16>().ok()),
        ) {
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
    let raft = (*start_node(
        node_id,
        members.clone(),
        Arc::clone(&coordinator),
        raft_dir,
        config.raft,
    )
    .await?)
        .clone();
    let leader: LeaderView = Arc::new(std::sync::RwLock::new(LeaderState::default()));
    spawn_leader_watcher(
        raft.clone(),
        node_id,
        Arc::clone(&leader),
        Arc::clone(&coordinator),
    );
    let writes: Arc<dyn WritePath> = Arc::new(RaftWrite::new(
        raft.clone(),
        node_id,
        members.clone(),
        Arc::clone(&leader),
    ));
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
    let raft_handler =
        seatunnel_engine_server::raft::network::RaftServiceHandler { raft: raft.clone() };

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
        let timeout_ms = config
            .worker_timeout_ms
            .max(config.worker_soft_timeout_ms)
            .max(1000);
        let cp_timeout_ms = config.checkpoint_timeout_ms.max(1000);
        let cancel = shutdown.clone();
        tokio::spawn(async move {
            let mut ticker = tokio::time::interval(Duration::from_millis(2000));
            // Owners present in the replicated state but absent from this
            // (leader-local) registry, and since when — a leader change
            // loses the previous registry, so dead owners must be released
            // through reconciliation rather than the heartbeat TTL alone.
            let mut missing_since: std::collections::HashMap<String, i64> =
                std::collections::HashMap::new();
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
                            // Durable release of the dead worker's tasks. Only the leader
                            // proposes — a follower's proposal is always forwarded away.
                            if writes.is_leader() {
                                let cmd =
                                    seatunnel_engine_server::job_coordinator::Command::EvictWorker {
                                        worker_id: worker_id.clone(),
                                    };
                                if let Err(e) = writes.propose(cmd).await {
                                    tracing::warn!("EvictWorker proposal failed: {}", e);
                                }
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

                        // Ownership reconciliation (leader only): task
                        // ownership lives in the replicated state, worker
                        // liveness in the leader-local registry. When the
                        // previous leader died, its embedded worker's tasks
                        // keep a dead owner nobody evicts — the orphan claim
                        // is then fenced as a "duplicate". Release owners
                        // that stay absent past the hard timeout; returning
                        // workers re-register and ADOPT their tasks first.
                        if writes.is_leader() {
                            let owners = coordinator.task_owning_workers();
                            let present: std::collections::HashSet<String> =
                                registry.read().unwrap().keys().cloned().collect();
                            for owner in owners {
                                if present.contains(&owner) {
                                    missing_since.remove(&owner);
                                } else {
                                    missing_since.entry(owner).or_insert(now);
                                }
                            }
                            let expired: Vec<String> = missing_since
                                .iter()
                                .filter(|(_, since)| now - **since > timeout_ms as i64)
                                .map(|(id, _)| id.clone())
                                .collect();
                            for worker_id in expired {
                                missing_since.remove(&worker_id);
                                let cmd =
                                    seatunnel_engine_server::job_coordinator::Command::EvictWorker {
                                        worker_id: worker_id.clone(),
                                    };
                                match writes.propose(cmd).await {
                                    Ok(_) => {
                                        tracing::warn!(
                                            "Leader reconcile: evicted owner {} absent from \
                                             the registry for > {}ms",
                                            worker_id,
                                            timeout_ms
                                        );
                                        wake.notify_waiters();
                                    }
                                    Err(e) => tracing::warn!(
                                        "EvictWorker proposal failed: {}",
                                        e
                                    ),
                                }
                            }
                        } else {
                            missing_since.clear();
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
    web: Option<WebConsoleArgs>,
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
    if let Some(web) = web {
        let endpoint = web
            .master
            .clone()
            .unwrap_or_else(|| advertise_address(advertise, &serving.local_addr));
        seatunnel_web::spawn_console(web.listen.clone(), endpoint, web_auth(&web));
    }
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
    web: Option<WebConsoleArgs>,
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
    if let Some(web) = web {
        // The console proxies to this node's own coordinator (loopback
        // always works even when the advertise address is not routable
        // from this process yet).
        let endpoint = web
            .master
            .clone()
            .unwrap_or_else(|| format!("127.0.0.1:{}", serving.local_addr.port()));
        seatunnel_web::spawn_console(web.listen.clone(), endpoint, web_auth(&web));
    }
    tracing::info!(
        "Hybrid node: coordinator at {} + in-process worker '{}'",
        advertise_addr,
        worker_id
    );

    // The in-process worker prefers this node's own coordinator, then the
    // other voters: on a multi-node hybrid cluster a follower's embedded
    // worker is redirected to the leader via the heartbeat leader-hint
    // (and can fail over through the list when the leader dies).
    let mut master_list = config.member_list.clone();
    master_list.retain(|m| m != &advertise_addr);
    master_list.insert(0, advertise_addr);
    let worker_bind_addr = format!("127.0.0.1:{}", serving.local_addr.port());
    // web = None: the console (if any) was already spawned above.
    let worker_fut = run_worker(&worker_bind_addr, master_list, worker_id, config, None);

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

/// Connect to `target` (a master address), re-register with running tasks
/// so reassigned tasks are fenced properly, and return the new client.
/// Used by master failover and leader-hint redirection alike; `None` means
/// the target is unreachable or rejected the registration.
async fn switch_master(
    target: &str,
    worker: &Arc<WorkerNode>,
    worker_id: &str,
    addr: &str,
    default_interval: u64,
    slots: u32,
) -> Option<MasterServiceClient<tonic::transport::Channel>> {
    let mut new_client = MasterServiceClient::connect(format!("http://{}", target))
        .await
        .map_err(|e| tracing::warn!("Cannot connect to master {}: {}", target, e))
        .ok()?;
    let running = worker.running_task_ids().await;
    let reg = tonic::Request::new(WorkerRegistration {
        worker_id: worker_id.to_string(),
        address: addr.to_string(),
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
            worker.set_master_client(new_client.clone()).await;
            Some(new_client)
        }
        Err(e) => {
            tracing::warn!("Re-registration with {} failed: {}", target, e);
            None
        }
    }
}

async fn run_worker(
    addr_arg: &str,
    master_list: Vec<String>,
    worker_id: &str,
    engine_config: EngineServerConfig,
    web: Option<WebConsoleArgs>,
) -> anyhow::Result<()> {
    let master = master_list.first().cloned().ok_or_else(|| {
        anyhow::anyhow!("no master address (use --master or the config member-list)")
    })?;

    // Embedded console (--web on a worker node): point it at the master
    // list so it fails over together with the heartbeat loop.
    if let Some(web) = web {
        let endpoint = web.master.clone().unwrap_or_else(|| master_list.join(","));
        seatunnel_web::spawn_console(web.listen.clone(), endpoint, web_auth(&web));
    }

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
    worker = worker.with_admission(
        seatunnel_engine_server::admission::AdmissionController::new(
            engine_config.admission_config(),
        ),
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
            let mut current_master = masters[0].clone();
            // A hint that failed to connect is not retried until the leader
            // hint changes — during an election window followers briefly
            // point at the DEAD previous leader.
            let mut bad_hint: Option<String> = None;
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

                // Long-poll budget + slack; a request that never returns
                // (dead channel) must degrade into a transport failure so
                // the failover machinery can rotate masters.
                let timeout = Duration::from_millis(interval_ms as u64 + 5_000);
                let result = tokio::time::timeout(timeout, client.heartbeat(hb)).await;
                let outcome = match result {
                    Ok(r) => r,
                    Err(_) => Err(tonic::Status::deadline_exceeded(format!(
                        "heartbeat to {} timed out after {}ms",
                        current_master,
                        timeout.as_millis()
                    ))),
                };
                match outcome {
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
                        // Leader redirect: a non-leader master serves no
                        // instructions and points at the leader via
                        // `leader_hint`. Follow it immediately — otherwise
                        // this worker would heartbeat a follower forever
                        // (the follower never refreshes its liveness entry,
                        // so it is evicted there while the leader never
                        // hears of it).
                        let hint = response.leader_hint;
                        if !hint.is_empty()
                            && hint != current_master
                            && bad_hint.as_ref() != Some(&hint)
                        {
                            tracing::info!(
                                "Master {} is a follower — following leader hint {}",
                                current_master,
                                hint
                            );
                            if let Some(new_client) = switch_master(
                                &hint,
                                &worker,
                                &worker_id,
                                &addr,
                                default_interval,
                                slots,
                            )
                            .await
                            {
                                client = new_client;
                                current_master = hint;
                                bad_hint = None;
                            } else {
                                bad_hint = Some(hint);
                            }
                        }
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
                            // switch_master re-registers with running tasks so
                            // the new master fences reassigned tasks properly.
                            if let Some(new_client) = switch_master(
                                &next,
                                &worker,
                                &worker_id,
                                &addr,
                                default_interval,
                                slots,
                            )
                            .await
                            {
                                client = new_client;
                                current_master = next;
                                tracing::info!("Failed over to master {}", current_master);
                            }
                            failures = 0; // rotate again next round on failure
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
