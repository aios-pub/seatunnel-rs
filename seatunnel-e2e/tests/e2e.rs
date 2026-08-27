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

//! End-to-end test: MySQL CDC → SeaTunnel cluster → Kafka.
//!
//! Requires the docker-compose infrastructure:
//!
//! ```bash
//! docker compose up -d zookeeper kafka mysql
//! cargo test -p seatunnel-e2e --test e2e -- --ignored --nocapture
//! ```
//!
//! When Kafka/MySQL are unreachable the test SKIPS so plain CI runs without
//! docker stay green.

use std::sync::Arc;
use std::time::Duration;

use seatunnel_engine_client::EngineClient;
use seatunnel_engine_comm::{
    generated::client_service_client::ClientServiceClient,
    generated::master_service_client::MasterServiceClient, SubmitJobRequest,
};
use seatunnel_engine_server::{
    new_worker_registry, ClientHandler, JobCoordinator, LocalStateStore, MasterHandler, WorkerNode,
};
use tokio::net::TcpStream;

const MYSQL: (&str, u16) = ("127.0.0.1", 13306);
const KAFKA: (&str, u16) = ("127.0.0.1", 9092);
const TOPIC: &str = "users-cdc-e2e-rs";

async fn reachable(addr: (&str, u16)) -> bool {
    TcpStream::connect(addr).await.is_ok()
}

async fn wait_for(addr: (&str, u16), secs: u64) -> bool {
    for _ in 0..secs * 2 {
        if reachable(addr).await {
            return true;
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
    false
}

async fn mysql_exec(sql: &str) -> anyhow::Result<()> {
    use tokio::process::Command;
    let out = Command::new("docker")
        .args([
            "exec",
            "seatunnel-rs-mysql-1",
            "mysql",
            "-uroot",
            "-proot",
            "-e",
            sql,
        ])
        .output()
        .await?;
    anyhow::ensure!(
        out.status.success(),
        "mysql exec failed: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    Ok(())
}

async fn kafka_consume(topic: &str) -> anyhow::Result<Vec<String>> {
    use tokio::process::Command;
    let out = Command::new("docker")
        .args([
            "exec",
            "seatunnel-rs-kafka-1",
            "kafka-console-consumer",
            "--bootstrap-server",
            "localhost:9092",
            "--topic",
            topic,
            "--from-beginning",
            "--timeout-ms",
            "6000",
        ])
        .output()
        .await?;
    Ok(String::from_utf8_lossy(&out.stdout)
        .lines()
        .filter(|l| l.starts_with('['))
        .map(str::to_string)
        .collect())
}

async fn spawn_master() -> anyhow::Result<String> {
    let coordinator = Arc::new(JobCoordinator::new());
    let registry = new_worker_registry();
    let handler = MasterHandler::new(coordinator.clone(), registry.clone());
    let client_handler = ClientHandler::new(coordinator, registry);

    // Bind eagerly; serve on the same listener.
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let addr = listener.local_addr()?.to_string();
    tokio::spawn(async move {
        use seatunnel_engine_comm::{
            generated::client_service_server::ClientServiceServer,
            generated::master_service_server::MasterServiceServer,
        };
        use tokio_stream::wrappers::TcpListenerStream;
        tonic::transport::Server::builder()
            .add_service(MasterServiceServer::new(handler))
            .add_service(ClientServiceServer::new(client_handler))
            .serve_with_incoming(TcpListenerStream::new(listener))
            .await
            .ok();
    });
    Ok(addr)
}

async fn spawn_worker(master_addr: &str) -> anyhow::Result<Arc<WorkerNode>> {
    let probe = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let worker_addr = probe.local_addr()?.to_string();
    drop(probe);

    let state_dir = std::env::temp_dir().join(format!("st-e2e-{}", uuid_like()));
    let worker = Arc::new(WorkerNode::new(
        "e2e-worker",
        &worker_addr,
        Arc::new(LocalStateStore::new(state_dir)),
    ));

    let mut client = MasterServiceClient::connect(format!("http://{}", master_addr)).await?;
    worker.set_master_client(client.clone()).await;
    client
        .register_worker(tonic::Request::new(
            seatunnel_engine_comm::WorkerRegistration {
                worker_id: "e2e-worker".into(),
                address: worker_addr,
                version: env!("CARGO_PKG_VERSION").into(),
                resources: Default::default(),
                heartbeat_interval_ms: 200,
                running_task_ids: Vec::new(),
            },
        ))
        .await?;

    let hb_worker = Arc::clone(&worker);
    let mut hb_client = client;
    tokio::spawn(async move {
        let mut tick = tokio::time::interval(Duration::from_millis(150));
        loop {
            tick.tick().await;
            let Ok(resp) = hb_client
                .heartbeat(seatunnel_engine_comm::HeartbeatRequest {
                    worker_id: "e2e-worker".into(),
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

fn uuid_like() -> String {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default();
    format!("{}{}", nanos.as_secs(), nanos.subsec_nanos())
}

/// Full loop: seed MySQL → submit CDC→Kafka job through gRPC → snapshot rows
/// land in Kafka → live insert lands in Kafka → cancel.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn mysql_cdc_to_kafka_closed_loop() {
    let deps_ok = tokio::join!(wait_for(MYSQL, 5), wait_for(KAFKA, 5));
    if !(deps_ok.0 && deps_ok.1) {
        eprintln!(
            "SKIP: kafka/mysql not reachable — start `docker compose up -d zookeeper kafka mysql`"
        );
        return;
    }

    let master_addr = spawn_master().await.unwrap();
    let _worker = spawn_worker(&master_addr).await.unwrap();

    // Seed a clean table.
    mysql_exec(
        "CREATE DATABASE IF NOT EXISTS seatunnel;
         USE seatunnel;
         DROP TABLE IF EXISTS users_e2e;
         CREATE TABLE users_e2e (id INT PRIMARY KEY AUTO_INCREMENT, name VARCHAR(64), score INT);
         INSERT INTO users_e2e(name,score) VALUES ('alice',90),('bob',85);",
    )
    .await
    .unwrap();

    // Submit the job over gRPC (same path as the CLI).
    let config = serde_json::json!({
        "env": { "job.name": "e2e-cdc-kafka", "parallelism": 1, "checkpoint.interval": 3000 },
        "source": {
            "MySQL-CDC": {
                "hostname": MYSQL.0, "port": MYSQL.1,
                "username": "root", "password": "root",
                "database-name": "seatunnel", "table-name": "users_e2e"
            }
        },
        "sink": {
            "Kafka": {
                "bootstrap.servers": format!("{}:{}", KAFKA.0, KAFKA.1),
                "topic": TOPIC, "format": "json", "batch.size": 10
            }
        }
    });
    let job_id = format!("job-{}", uuid_like());
    let mut grpc = ClientServiceClient::connect(format!("http://{}", master_addr))
        .await
        .unwrap();
    let resp = grpc
        .submit_job(tonic::Request::new(SubmitJobRequest {
            job_id: job_id.clone(),
            job_config: serde_json::to_vec(&config).unwrap(),
            parallelism: 0,
            user: "e2e".into(),
            job_name: "e2e-cdc-kafka".into(),
        }))
        .await
        .unwrap()
        .into_inner();
    assert!(resp.success, "submit failed: {}", resp.message);

    // Wait for both snapshot rows in Kafka.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(60);
    loop {
        assert!(
            tokio::time::Instant::now() < deadline,
            "snapshot rows never reached Kafka"
        );
        let messages = kafka_consume(TOPIC).await.unwrap_or_default();
        if messages.iter().any(|m| m.contains("alice"))
            && messages.iter().any(|m| m.contains("bob"))
        {
            break;
        }
        tokio::time::sleep(Duration::from_millis(1500)).await;
    }
    println!("snapshot rows verified");

    // Live insert must reach Kafka through the incremental stream.
    mysql_exec("USE seatunnel; INSERT INTO users_e2e(name,score) VALUES ('zoe',100);")
        .await
        .unwrap();
    let deadline = tokio::time::Instant::now() + Duration::from_secs(60);
    loop {
        assert!(
            tokio::time::Instant::now() < deadline,
            "incremental insert never reached Kafka"
        );
        let messages = kafka_consume(TOPIC).await.unwrap_or_default();
        if messages.iter().any(|m| m.contains("zoe")) {
            println!("incremental row verified");
            break;
        }
        tokio::time::sleep(Duration::from_millis(1500)).await;
    }

    // Cancel the streaming job.
    let client = EngineClient::new(&master_addr);
    client.cancel_job(&job_id).await.unwrap();
    println!("cancelled {}", job_id);
}
