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

//! In-process cluster integration test.
//!
//! Boots a real master (gRPC) + real worker + heartbeat loop inside this
//! test process, submits a job over gRPC exactly like the CLI would, and
//! asserts the full scheduling loop:
//!
//!   submit → master schedules → worker pulls via heartbeat → executes
//!   chained Source→Sink → reports status → job reaches terminal state.

use std::sync::Arc;
use std::time::Duration;

use seatunnel_engine_client::EngineClient;
use seatunnel_engine_comm::{
    generated::client_service_server::ClientServiceServer,
    generated::master_service_server::MasterServiceServer, SubmitJobRequest,
};
use seatunnel_engine_server::{
    new_worker_registry, ClientHandler, JobCoordinator, LocalStateStore, MasterHandler, WorkerNode,
};

async fn spawn_master() -> anyhow::Result<(String, Arc<JobCoordinator>)> {
    let _ = tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "warn".into()),
        )
        .with_test_writer()
        .try_init();
    let coordinator = Arc::new(JobCoordinator::new());
    let registry = new_worker_registry();
    let handler = MasterHandler::new(coordinator.clone(), registry.clone());
    let client = ClientHandler::new(coordinator.clone(), registry);

    // Bind eagerly so callers can connect immediately after this returns.
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let addr = listener.local_addr()?;
    tokio::spawn(async move {
        tonic::transport::Server::builder()
            .add_service(MasterServiceServer::new(handler))
            .add_service(ClientServiceServer::new(client))
            .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(listener))
            .await
            .expect("gRPC server failed");
    });
    Ok((addr.to_string(), coordinator))
}

async fn spawn_worker(master_addr: &str) -> anyhow::Result<Arc<WorkerNode>> {
    // Bind an ephemeral port for the worker's advertised address.
    let probe = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let worker_addr = probe.local_addr()?.to_string();
    drop(probe);

    let state_dir =
        std::env::temp_dir().join(format!("st-it-worker-{}", uuid::Uuid::new_v4().simple()));
    let state_store = Arc::new(LocalStateStore::new(state_dir));
    let worker = Arc::new(WorkerNode::new("it-worker-1", &worker_addr, state_store));

    let mut client =
        seatunnel_engine_comm::generated::master_service_client::MasterServiceClient::connect(
            format!("http://{}", master_addr),
        )
        .await?;
    worker.set_master_client(client.clone()).await;
    client
        .register_worker(tonic::Request::new(
            seatunnel_engine_comm::WorkerRegistration {
                worker_id: "it-worker-1".into(),
                address: worker_addr,
                version: "test".into(),
                resources: Default::default(),
                heartbeat_interval_ms: 200,
            },
        ))
        .await?;

    // Heartbeat loop (fast interval for tests).
    let hb_worker = Arc::clone(&worker);
    let mut hb_client = client;
    tokio::spawn(async move {
        let mut tick = tokio::time::interval(Duration::from_millis(150));
        loop {
            tick.tick().await;
            let Ok(resp) = hb_client
                .heartbeat(seatunnel_engine_comm::HeartbeatRequest {
                    worker_id: "it-worker-1".into(),
                    address: String::new(),
                    timestamp: seatunnel_engine_core::now_millis(),
                    tasks: vec![],
                })
                .await
            else {
                break;
            };
            let response = resp.into_inner();
            if !response.cancel_jobs.is_empty() {
                hb_worker.cancel_jobs(&response.cancel_jobs).await;
            }
            for task in response.pending_tasks {
                hb_worker.assign_task(task).await;
            }
        }
    });

    Ok(worker)
}

fn submit_request(job_name: &str, config: serde_json::Value) -> SubmitJobRequest {
    SubmitJobRequest {
        job_id: format!("job-{}", uuid::Uuid::new_v4()),
        job_config: serde_json::to_vec(&config).unwrap(),
        parallelism: 0, // use env parallelism
        user: "it".into(),
        job_name: job_name.into(),
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn bounded_job_runs_to_completed_through_cluster() {
    let (master_addr, _coordinator) = spawn_master().await.unwrap();
    let _worker = spawn_worker(&master_addr).await.unwrap();

    let client = EngineClient::new(&master_addr);

    let config = serde_json::json!({
        "env": { "job.name": "it-bounded", "parallelism": 2 },
        "source": { "Fake": { "row.num": 5 } },
        "sink": { "Console": {} }
    });

    // Submit through the raw gRPC service (mirrors EngineClient.submit_job).
    let mut grpc =
        seatunnel_engine_comm::generated::client_service_client::ClientServiceClient::connect(
            format!("http://{}", master_addr),
        )
        .await
        .unwrap();
    let resp = grpc
        .submit_job(tonic::Request::new(submit_request("it-bounded", config)))
        .await
        .unwrap();
    let resp = resp.into_inner();
    assert!(resp.success, "submit failed: {}", resp.message);
    let job_id = resp.job_id;

    // Poll until terminal.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(15);
    loop {
        assert!(
            tokio::time::Instant::now() < deadline,
            "job did not finish in time"
        );
        let status = client.get_job_status(&job_id).await.expect("status");
        match status.state {
            4 => {
                let records: i64 = status.tasks.iter().map(|t| t.processed_records).sum();
                assert_eq!(records, 10, "2 subtasks × 5 rows expected");
                break;
            }
            5 | 6 => panic!("job ended abnormally: {:?}", status.error_message),
            _ => tokio::time::sleep(Duration::from_millis(100)).await,
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn streaming_job_cancels_to_cancelled_state() {
    let (master_addr, _) = spawn_master().await.unwrap();
    let _worker = spawn_worker(&master_addr).await.unwrap();

    let client = EngineClient::new(&master_addr);

    let config = serde_json::json!({
        "env": { "job.name": "it-streaming", "parallelism": 1 },
        // A Kafka source against a closed port streams forever (Empty polls).
        "source": { "Kafka": { "bootstrap.servers": "127.0.0.1:19092", "topic": "never" } },
        "sink": { "Console": {} }
    });

    let mut grpc =
        seatunnel_engine_comm::generated::client_service_client::ClientServiceClient::connect(
            format!("http://{}", master_addr),
        )
        .await
        .unwrap();
    let resp = grpc
        .submit_job(tonic::Request::new(submit_request("it-streaming", config)))
        .await
        .unwrap()
        .into_inner();
    assert!(resp.success);
    let job_id = resp.job_id;

    // Wait until the job is RUNNING on the worker.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    loop {
        assert!(tokio::time::Instant::now() < deadline, "job never started");
        let s = client.get_job_status(&job_id).await.unwrap();
        if s.state == 3 {
            break;
        }
        assert_ne!(s.state, 5, "job failed to start");
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    // Cancel and observe CANCELLED.
    client.cancel_job(&job_id).await.expect("cancel");
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    loop {
        assert!(
            tokio::time::Instant::now() < deadline,
            "cancel not observed"
        );
        let s = client.get_job_status(&job_id).await.unwrap();
        if s.state == 6 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

#[tokio::test]
async fn submit_without_workers_is_rejected() {
    let (master_addr, _) = spawn_master().await.unwrap();
    // Wait for the gRPC listener to accept connections.
    let mut grpc = loop {
        match seatunnel_engine_comm::generated::client_service_client::ClientServiceClient::connect(
            format!("http://{}", master_addr),
        )
        .await
        {
            Ok(c) => break c,
            Err(_) => tokio::time::sleep(Duration::from_millis(50)).await,
        }
    };
    let result = grpc
        .submit_job(tonic::Request::new(submit_request(
            "no-workers",
            serde_json::json!({
                "env": {"parallelism": 1},
                "source": {"Fake": {"row.num": 1}},
                "sink": {"Console": {}}
            }),
        )))
        .await;
    assert!(result.is_err(), "expected rejection with zero workers");
}
