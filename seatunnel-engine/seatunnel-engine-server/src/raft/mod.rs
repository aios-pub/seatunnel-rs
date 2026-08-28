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

//! Consensus-based HA: the coordinator's durable state is an openraft
//! state machine (`Command` log + `export_state` snapshots).
//!
//! What this replaces and guarantees:
//! - The ordered member-list warm standby (snapshot polling) is gone;
//!   a Raft majority elects the leader in ~1s, so a deposed master
//!   cannot commit anything and dual masters are impossible by
//!   construction (the Java engine has neither quorum nor fencing).
//! - The Raft leader term IS the wire fencing term of Stage 1: workers
//!   reject instructions from any lower term.
//! - Worker liveness stays leader-local (heartbeats); workers
//!   re-register when the leader changes, so the volatile registry is
//!   rebuilt within one heartbeat.

pub mod log_store;
pub mod network;
pub mod state_machine;

use std::collections::BTreeMap;
use std::io::Cursor;
use std::sync::Arc;

use openraft::BasicNode;
use tracing::{info, warn};

use crate::job_coordinator::{Command, CommandResult, JobCoordinator};

openraft::declare_raft_types!(
    /// Types of this engine's raft instance: commands as log payloads.
    pub Types:
        D = Command,
        R = CommandResult,
        Node = BasicNode
);

pub use openraft::raft::ClientWriteResult;

/// Shared handle to a running raft node (cheap to clone).
pub type Raft = openraft::Raft<Types>;

/// Stable per-node identity: voters are the ordered member list, ids
/// 1-based. The map is static for the life of the process.
pub type Members = BTreeMap<u64, BasicNode>;

/// Validate a member list for raft: at least one voter, and more than
/// one voter must form a quorum-able (odd) count. Two voters can never
/// reach majority after either fails — reject loudly instead of running
/// a silently split-brain-prone pair.
pub fn validate_voters(count: usize) -> anyhow::Result<()> {
    if count == 0 {
        anyhow::bail!("empty member list: at least one master/hybrid node is required");
    }
    if count > 1 && count % 2 == 0 {
        anyhow::bail!(
            "{} masters configured: voter counts must be odd (1, 3, 5, ...) — \
             two voters cannot reach a majority after either node fails, which \
             would reintroduce the dual-master risk this design eliminates",
            count
        );
    }
    Ok(())
}

/// Build the member map from the configured addresses (id = 1..n).
pub fn members_from_addresses(addresses: &[String]) -> Members {
    addresses
        .iter()
        .enumerate()
        .map(|(i, addr)| (i as u64 + 1, BasicNode { addr: addr.clone() }))
        .collect()
}

/// Create and bootstrap a raft node over the coordinator state machine.
///
/// Bootstrap rule: the first member initializes the cluster membership;
/// later members join by receiving heartbeats/votes (they retry
/// `initialize` once after a delay in case member 1 never comes back).
pub async fn start_node(
    node_id: u64,
    members: Members,
    coordinator: Arc<JobCoordinator>,
    dir: std::path::PathBuf,
) -> anyhow::Result<Arc<Raft>> {
    let config = openraft::Config {
        cluster_name: "seatunnel-engine".to_string(),
        // Fast failover (~1s election) is safe BECAUSE there is a quorum:
        // the Java engine needs 180s only because it has none.
        election_timeout_min: 800,
        election_timeout_max: 1_600,
        heartbeat_interval: 150,
        install_snapshot_timeout: 5_000,
        snapshot_policy: openraft::SnapshotPolicy::LogsSinceLast(
            SNAPSHOT_EVERY_N_LOGS.try_into().expect("positive"),
        ),
        max_payload_entries: 256,
        ..Default::default()
    };
    let config = Arc::new(config.validate()?);

    std::fs::create_dir_all(&dir)?;
    let log_store = log_store::FileLogStore::new(&dir)?;
    let state_machine = state_machine::CoordinatorStateMachine::new(coordinator, &dir).await?;

    let network = network::GrpcNetworkFactory {
        members: members.clone(),
    };
    let raft = openraft::Raft::new(node_id, config, network, log_store, state_machine).await?;

    // Membership bootstrap: node 1 owns it; later members join via
    // heartbeats/votes. If member 1 never appears, one of them retries
    // initialization after a delay so the cluster is not stranded.
    let already_initialized = raft
        .metrics()
        .borrow()
        .membership_config
        .membership()
        .nodes()
        .next()
        .is_some();
    if !already_initialized {
        if node_id == 1 {
            match raft.initialize(members.clone()).await {
                Ok(_) => info!("Raft: initialized cluster with {} voter(s)", members.len()),
                Err(openraft::error::RaftError::APIError(
                    openraft::error::InitializeError::NotAllowed(_),
                )) => info!("Raft: cluster already initialized"),
                Err(e) => return Err(anyhow::anyhow!("raft initialize: {}", e)),
            }
        } else {
            tokio::spawn({
                let raft = raft.clone();
                let members = members.clone();
                async move {
                    tokio::time::sleep(std::time::Duration::from_secs(5)).await;
                    let empty = raft
                        .metrics()
                        .borrow()
                        .membership_config
                        .membership()
                        .nodes()
                        .next()
                        .is_none();
                    if empty {
                        match raft.initialize(members).await {
                            Ok(_) => {
                                warn!("Raft: initialized by non-first member (member 1 absent)")
                            }
                            Err(_) => {}
                        }
                    }
                }
            });
        }
    }

    Ok(Arc::new(raft))
}

/// Logs between automatic snapshots (control-plane volume is tiny).
const SNAPSHOT_EVERY_N_LOGS: u64 = 4_096;

// ---------------------------------------------------------------------------
// Write path: handlers mutate coordinator state ONLY through this trait.
// Direct mode applies in place (tests, single-process); consensus mode
// goes through the Raft log and returns after the local apply — so a
// follower/leader-switch transparently fails with a leader hint.
// ---------------------------------------------------------------------------

/// Shared leader view updated by the metrics watcher task.
pub type LeaderView = Arc<std::sync::RwLock<LeaderState>>;

#[derive(Debug, Clone, Default)]
pub struct LeaderState {
    pub term: u64,
    /// Leader node id (None while electing).
    pub leader_id: Option<u64>,
}

/// How a handler persists a coordinator mutation.
#[async_trait::async_trait]
pub trait WritePath: Send + Sync {
    /// Apply a command durably; returns after the local state machine
    /// applied it (Direct: immediately; Raft: after commit+apply).
    async fn propose(&self, cmd: Command) -> anyhow::Result<CommandResult>;

    /// Whether this node may serve writes/dispatches right now.
    fn is_leader(&self) -> bool {
        true
    }

    /// Address of the current leader ("" = this node or unknown).
    fn leader_hint(&self) -> String {
        String::new()
    }
}

/// In-process write path: apply immediately (tests, embedded setups).
pub struct DirectWrite {
    coordinator: Arc<JobCoordinator>,
}

impl DirectWrite {
    pub fn new(coordinator: Arc<JobCoordinator>) -> Self {
        DirectWrite { coordinator }
    }
}

#[async_trait::async_trait]
impl WritePath for DirectWrite {
    async fn propose(&self, cmd: Command) -> anyhow::Result<CommandResult> {
        Ok(self.coordinator.apply_command(&cmd))
    }
}

/// Consensus write path: `Raft::client_write` and await the apply.
pub struct RaftWrite {
    raft: Raft,
    my_id: u64,
    members: Members,
    leader: LeaderView,
}

impl RaftWrite {
    pub fn new(raft: Raft, my_id: u64, members: Members, leader: LeaderView) -> Self {
        RaftWrite {
            raft,
            my_id,
            members,
            leader,
        }
    }
}

#[async_trait::async_trait]
impl WritePath for RaftWrite {
    async fn propose(&self, cmd: Command) -> anyhow::Result<CommandResult> {
        let result = self.raft.client_write(cmd).await?;
        Ok(result.data)
    }

    fn is_leader(&self) -> bool {
        self.leader
            .read()
            .unwrap()
            .leader_id
            .map(|l| l == self.my_id)
            .unwrap_or(false)
    }

    fn leader_hint(&self) -> String {
        let view = self.leader.read().unwrap();
        match view.leader_id {
            Some(id) if id != self.my_id => self
                .members
                .get(&id)
                .map(|n| n.addr.clone())
                .unwrap_or_default(),
            _ => String::new(),
        }
    }
}

/// Watch raft metrics into the shared leader view + the coordinator's
/// fencing term (the wire term IS the raft term).
pub fn spawn_leader_watcher(raft: Raft, my_id: u64, leader: LeaderView, coordinator: Arc<JobCoordinator>) {
    tokio::spawn(async move {
        let mut rx = raft.metrics();
        loop {
            {
                let m = rx.borrow_and_update();
                let mut view = leader.write().unwrap();
                view.term = m.current_term;
                view.leader_id = m.current_leader;
            }
            let term = rx.borrow().current_term;
            // The wire fencing term equals the raft term — workers that
            // saw a higher term reject any deposed master automatically.
            coordinator.observe_term(term);
            if rx.changed().await.is_err() {
                break;
            }
        }
        let _ = my_id;
    });
}
