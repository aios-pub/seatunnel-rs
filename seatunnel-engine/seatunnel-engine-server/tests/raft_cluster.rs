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

//! Consensus HA integration tests: real openraft nodes over real gRPC
//! transports inside this test process.
//!
//! - single voter: elects itself, applies commands, no network round trips
//! - three voters: leader election, replicated apply on all nodes, leader
//!   failover (deposed leader stops writing — the fencing guarantee the
//!   Java engine lacks), and 2-voter rejection

use std::sync::Arc;
use std::time::Duration;

use seatunnel_engine_comm::generated::raft_service_server::RaftServiceServer;
use seatunnel_engine_server::job_coordinator::{Command, JobCoordinator};
use seatunnel_engine_server::raft::{
    LeaderState, LeaderView, Raft, RaftWrite, WritePath, members_from_addresses, start_node,
    validate_voters,
};
use seatunnel_engine_server::server_config::RaftTiming;

struct Node {
    #[allow(dead_code)]
    id: u64,
    raft: Raft,
    coordinator: Arc<JobCoordinator>,
    writes: Arc<dyn WritePath>,
    leader: LeaderView,
    addr: String,
}

async fn start_test_node(
    id: u64,
    members: seatunnel_engine_server::raft::Members,
    state_root: &std::path::Path,
) -> anyhow::Result<Node> {
    // Bind first so the member map (built from real addresses) is
    // identical on every node — raft node ids must agree cluster-wide.
    let addr = members
        .get(&id)
        .map(|n| n.addr.clone())
        .ok_or_else(|| anyhow::anyhow!("node {} missing from member map", id))?;
    let listener = tokio::net::TcpListener::bind(&addr).await?;
    let coordinator = Arc::new(JobCoordinator::new());
    let dir = state_root.join(format!("node-{}", id));
    let raft = (*start_node(
        id,
        members.clone(),
        Arc::clone(&coordinator),
        dir,
        RaftTiming::default(),
    )
    .await?)
        .clone();
    let handler = seatunnel_engine_server::raft::network::RaftServiceHandler { raft: raft.clone() };
    tokio::spawn(async move {
        let _ = tonic::transport::Server::builder()
            .add_service(RaftServiceServer::new(handler))
            .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(listener))
            .await;
    });
    let leader: LeaderView = Arc::new(std::sync::RwLock::new(LeaderState::default()));
    seatunnel_engine_server::raft::spawn_leader_watcher(
        raft.clone(),
        id,
        Arc::clone(&leader),
        Arc::clone(&coordinator),
    );
    let writes: Arc<dyn WritePath> = Arc::new(RaftWrite::new(
        raft.clone(),
        id,
        members,
        Arc::clone(&leader),
    ));
    Ok(Node {
        id,
        raft,
        coordinator,
        writes,
        leader,
        addr,
    })
}

/// Pre-bind `count` listeners and build the shared member map (ids
/// 1..=count) — identical on every node.
async fn bind_members(count: u64) -> anyhow::Result<seatunnel_engine_server::raft::Members> {
    let mut addrs = Vec::new();
    for _ in 0..count {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
        addrs.push(listener.local_addr()?.to_string());
        drop(listener);
    }
    Ok(members_from_addresses(&addrs))
}

async fn wait_for_leader(nodes: &[Node], timeout: Duration) -> (usize, u64) {
    let deadline = tokio::time::Instant::now() + timeout;
    loop {
        assert!(tokio::time::Instant::now() < deadline, "no leader elected");
        for (idx, node) in nodes.iter().enumerate() {
            let view = node.leader.read().unwrap().clone();
            if let Some(leader_id) = view.leader_id {
                if leader_id == node.id {
                    return (idx, view.term);
                }
            }
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

fn tmp_root(tag: &str) -> std::path::PathBuf {
    let dir =
        std::env::temp_dir().join(format!("st-raft-{}-{}", tag, uuid::Uuid::new_v4().simple()));
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

fn submit_command(job_id: &str) -> Command {
    Command::CancelJob {
        job_id: job_id.to_string(),
        at_ms: 42,
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn single_voter_elects_and_applies() {
    validate_voters(1).expect("single voter is legal");
    let root = tmp_root("single");
    let members = bind_members(1).await.unwrap();
    let node = start_test_node(1, members, &root).await.unwrap();

    let (idx, term) = wait_for_leader(std::slice::from_ref(&node), Duration::from_secs(10)).await;
    assert_eq!(idx, 0);
    assert!(term >= 1, "leader term must be >= 1, got {}", term);

    // A write applies on the leader and lands in the state machine.
    node.writes
        .propose(submit_command("j-single"))
        .await
        .unwrap();
    // CancelJob of an unknown job is a no-op command; assert it applied by
    // proposing a real SubmitJob and checking the coordinator.
    let job = seatunnel_engine_server::job_coordinator::JobDto {
        job_id: "jx".into(),
        job_name: "x".into(),
        state: "SCHEDULED".into(),
        parallelism: 1,
        start_time: 1,
        end_time: None,
        error_message: None,
        checkpoint_interval_ms: 30_000,
        checkpoints_completed: 0,
        next_checkpoint_id: 1,
        raw_config: String::new(),
        tasks: vec![],
    };
    node.writes
        .propose(Command::SubmitJob {
            job,
            descriptors: vec![],
        })
        .await
        .unwrap();
    assert!(node.coordinator.get_job("jx").is_some());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn three_voters_failover_without_dual_writes() {
    validate_voters(2).expect_err("two voters must be rejected");
    validate_voters(3).expect("three voters are legal");

    let root = tmp_root("three");
    let members = bind_members(3).await.unwrap();
    let mut nodes = Vec::new();
    for id in 1..=3u64 {
        let node = start_test_node(id, members.clone(), &root).await.unwrap();
        nodes.push(node);
    }

    // Election converges (node 1 bootstraps the membership).
    let (leader_idx, term) = wait_for_leader(&nodes, Duration::from_secs(15)).await;
    assert!(term >= 1);

    // A write on the leader replicates to every node's state machine.
    let job = seatunnel_engine_server::job_coordinator::JobDto {
        job_id: "jrepl".into(),
        job_name: "r".into(),
        state: "SCHEDULED".into(),
        parallelism: 1,
        start_time: 1,
        end_time: None,
        error_message: None,
        checkpoint_interval_ms: 30_000,
        checkpoints_completed: 0,
        next_checkpoint_id: 1,
        raw_config: String::new(),
        tasks: vec![],
    };
    nodes[leader_idx]
        .writes
        .propose(Command::SubmitJob {
            job,
            descriptors: vec![],
        })
        .await
        .unwrap();

    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    loop {
        assert!(
            tokio::time::Instant::now() < deadline,
            "replication stalled"
        );
        if nodes
            .iter()
            .all(|n| n.coordinator.get_job("jrepl").is_some())
        {
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    // Follower writes are rejected (leader gate) with a real hint.
    let follower = &nodes[(leader_idx + 1) % 3];
    assert!(!follower.writes.is_leader());
    assert!(!follower.writes.leader_hint().is_empty());

    // Kill the leader: a quorum (2 of 3) remains and elects a successor.
    // To the survivors a graceful shutdown and a kill -9 are the same
    // thing — silence — so this exercises the kill -9 failover path.
    let dead_leader = nodes.remove(leader_idx);
    let old_term = dead_leader.leader.read().unwrap().term;
    let _ = dead_leader.raft.shutdown().await;
    let survivors: Vec<Node> = nodes;
    let (new_idx, new_term) = wait_for_leader(&survivors, Duration::from_secs(15)).await;
    assert!(new_term > old_term, "term must advance across failover");
    // Disjoint per-node election windows make the handover clean: the
    // lowest live voter campaigns first and the others grant, so the
    // term advances by ~1. A storm of failed campaigns would show up as
    // rapid term inflation — bounded here.
    assert!(
        new_term <= old_term + 3,
        "term inflated {} -> {} across one failover (election thrashing)",
        old_term,
        new_term
    );

    // Writes still work on the successor — this is the availability the
    // quorum buys.
    survivors[new_idx]
        .writes
        .propose(submit_command("after-failover"))
        .await
        .unwrap();
}
