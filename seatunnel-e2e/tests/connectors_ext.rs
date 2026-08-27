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

//! End-to-end tests for the RabbitMQ / HTTP / ClickHouse connectors.
//!
//! Drives REAL `seatunnel run -m local` child processes against the
//! docker-compose infrastructure:
//!
//! ```bash
//! cargo build -p seatunnel-cli          # the tests spawn target/debug/seatunnel
//! docker compose up -d rabbitmq clickhouse
//! cargo test -p seatunnel-e2e --test connectors_ext -- --nocapture
//! ```
//!
//! Covered flows:
//! - ClickHouse (source)  → RabbitMQ (sink): pk-cursor read + confirmed publishes
//! - HTTP (source)       → ClickHouse (sink): data-path extraction + JSONEachRow insert
//! - RabbitMQ (source)   → HTTP (sink): deferred-ack consumption + per-row POST
//!
//! When the required services (or the CLI binary) are unreachable the
//! affected test SKIPS so plain CI runs without docker stay green.

use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use futures::StreamExt;
use tokio::net::TcpStream;
use tokio::process::{Child, Command};

const RABBITMQ: (&str, u16) = ("127.0.0.1", 5672);
const CLICKHOUSE: (&str, u16) = ("127.0.0.1", 8123);

fn cli_binary() -> Option<PathBuf> {
    // Manifest is <repo>/seatunnel-e2e/tests → binary at <repo>/target/debug.
    let mut dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    dir.pop(); // seatunnel-e2e
    let bin = dir.join("target/debug/seatunnel");
    bin.exists().then_some(bin)
}

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

fn unique_suffix() -> String {
    let ts = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis())
        .unwrap_or_default();
    format!("{}_{}", std::process::id(), ts)
}

fn temp_path(name: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!("st-e2e-{}-{}", name, unique_suffix()));
    std::fs::create_dir_all(&dir).expect("create temp dir");
    dir
}

fn write_job(path: &Path, body: &str) {
    std::fs::write(path, body).expect("write job config");
}

// ---------------------------------------------------------------------------
// Infrastructure helpers
// ---------------------------------------------------------------------------

async fn ch_query(sql: &str) -> anyhow::Result<String> {
    let text = reqwest::Client::new()
        .get("http://127.0.0.1:8123/")
        .query(&[("query", sql)])
        .send()
        .await?
        .error_for_status()?
        .text()
        .await?;
    Ok(text)
}

async fn ch_exec(sql: &str) -> anyhow::Result<()> {
    ch_query(sql).await.map(|_| ())
}

async fn rabbitmq_channel() -> anyhow::Result<lapin::Channel> {
    let conn = lapin::Connection::connect(
        "amqp://guest:guest@127.0.0.1:5672/%2F",
        lapin::ConnectionProperties::default(),
    )
    .await?;
    let channel = conn.create_channel().await?;
    let _ = conn; // keep the connection alive through the channel
    Ok(channel)
}

/// In-process HTTP stub: GET /items serves a fixed JSON document, POST
/// /collect counts requests.
async fn spawn_api(collector: Arc<AtomicUsize>, items_body: String) -> anyhow::Result<String> {
    let doc: serde_json::Value = serde_json::from_str(&items_body)?;
    let app = axum::Router::new()
        .route(
            "/items",
            axum::routing::get(move || {
                let doc = doc.clone();
                async move { axum::Json(doc) }
            }),
        )
        .route(
            "/collect",
            axum::routing::post(move || {
                let collector = collector.clone();
                async move {
                    collector.fetch_add(1, Ordering::SeqCst);
                    axum::http::StatusCode::OK
                }
            }),
        );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let addr = listener.local_addr()?;
    tokio::spawn(async move {
        if let Err(e) = axum::serve(listener, app).await {
            tracing::warn!("stub api stopped: {}", e);
        }
    });
    Ok(format!("http://{}", addr))
}

/// Spawn `seatunnel run -m local` for a job config; log to the given path.
fn start_job(binary: &Path, job: &Path, state_dir: &Path, log: &Path) -> anyhow::Result<Child> {
    let log_file = std::fs::File::create(log).expect("create log");
    Ok(Command::new(binary)
        .args([
            "run",
            "-c",
            job.to_str().unwrap(),
            "-m",
            "local",
            "--state-dir",
            state_dir.to_str().unwrap(),
        ])
        .env("RUST_LOG", "info")
        .stdout(Stdio::from(log_file.try_clone()?))
        .stderr(Stdio::from(log_file))
        .spawn()?)
}

async fn wait_job(mut child: Child, log: &Path) -> anyhow::Result<()> {
    let status = tokio::time::timeout(Duration::from_secs(120), child.wait()).await??;
    if !status.success() {
        anyhow::bail!(
            "job exited with {:?}; last logs:\n{}",
            status.code(),
            log_tail(log, 20)
        );
    }
    Ok(())
}

async fn kill_job(mut child: Child) {
    let _ = child.kill().await;
    let _ = child.wait().await;
}

fn log_tail(log: &Path, lines: usize) -> String {
    let all: Vec<String> = std::fs::read_to_string(log)
        .unwrap_or_default()
        .lines()
        .map(str::to_string)
        .collect();
    let start = all.len().saturating_sub(lines);
    all[start..].join("\n")
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[tokio::test]
async fn clickhouse_source_to_rabbitmq_sink() -> anyhow::Result<()> {
    let binary = match (
        cli_binary(),
        wait_for(CLICKHOUSE, 1).await,
        wait_for(RABBITMQ, 1).await,
    ) {
        (Some(binary), true, true) => binary,
        _ => {
            eprintln!("SKIP: clickhouse/rabbitmq or CLI binary unreachable");
            return Ok(());
        }
    };
    let suffix = unique_suffix();
    let table = format!("e2e_ch2rmq_{suffix}");
    let queue = format!("e2e_ch2rmq_{suffix}");
    let dir = temp_path("ch2rmq");

    // Seed the source table.
    ch_exec(&format!(
        "CREATE TABLE default.`{table}` (id Int64, name String) ENGINE = MergeTree ORDER BY id"
    ))
    .await?;
    let mut rows = String::new();
    for i in 0..25i64 {
        rows.push_str(&format!("{{\"id\":{i},\"name\":\"u{i}\"}}\n"));
    }
    reqwest::Client::new()
        .post("http://127.0.0.1:8123/")
        .query(&[(
            "query",
            format!("INSERT INTO default.`{table}` FORMAT JSONEachRow").as_str(),
        )])
        .body(rows)
        .send()
        .await?
        .error_for_status()?;

    // Declare + purge the target queue.
    let channel = rabbitmq_channel().await?;
    channel
        .queue_declare(
            queue.as_str().into(),
            lapin::options::QueueDeclareOptions {
                durable: true,
                ..Default::default()
            },
            lapin::types::FieldTable::default(),
        )
        .await?;
    let _ = channel
        .queue_purge(
            queue.as_str().into(),
            lapin::options::QueuePurgeOptions::default(),
        )
        .await;

    let job = dir.join("job.yaml");
    write_job(
        &job,
        &format!(
            r#"
env:
  job.name: ch2rmq-e2e
  parallelism: 1

source:
  ClickHouse:
    url: http://127.0.0.1:8123
    database: default
    table: {table}
    username: default
    password: ""
    primary-keys: id
    fetch-size: 10

sink:
  RabbitMQ:
    host: 127.0.0.1
    port: 5672
    username: guest
    password: guest
    queue-name: {queue}
    format: json
"#
        ),
    );
    let log = dir.join("job.log");
    let child = start_job(&binary, &job, &dir, &log)?;
    wait_job(child, &log).await?;

    // Consume everything the sink published.
    let consumer = channel
        .basic_consume(
            queue.as_str().into(),
            "e2e-verifier".into(),
            lapin::options::BasicConsumeOptions {
                no_ack: true,
                ..Default::default()
            },
            lapin::types::FieldTable::default(),
        )
        .await?;
    let mut payloads = Vec::new();
    let consume = async {
        let mut stream = consumer;
        while payloads.len() < 25 {
            match tokio::time::timeout(Duration::from_secs(15), stream.next()).await {
                Ok(Some(Ok(delivery))) => {
                    payloads.push(String::from_utf8_lossy(&delivery.data).to_string())
                }
                Ok(Some(Err(e))) => anyhow::bail!("consume error: {}", e),
                Ok(None) => anyhow::bail!("consumer stream ended early"),
                Err(_) => anyhow::bail!("timed out after {} message(s)", payloads.len()),
            }
        }
        anyhow::Ok(())
    };
    consume.await?;
    assert_eq!(payloads.len(), 25);
    let first: serde_json::Value = serde_json::from_str(&payloads[0])?;
    assert!(first.is_array(), "positional JSON array payload expected");
    assert_eq!(first[0], serde_json::json!(0));

    let _ = ch_exec(&format!("DROP TABLE IF EXISTS default.`{table}`")).await;
    Ok(())
}

#[tokio::test]
async fn http_source_to_clickhouse_sink() -> anyhow::Result<()> {
    let binary = match (cli_binary(), wait_for(CLICKHOUSE, 1).await) {
        (Some(binary), true) => binary,
        _ => {
            eprintln!("SKIP: clickhouse or CLI binary unreachable");
            return Ok(());
        }
    };
    let suffix = unique_suffix();
    let table = format!("e2e_http2ch_{suffix}");
    let dir = temp_path("http2ch");

    // In-process API serving 30 items inside a wrapper document.
    let mut items = Vec::new();
    for i in 0..30i64 {
        items.push(serde_json::json!({"id": i, "name": format!("u{i}")}));
    }
    let api = spawn_api(
        Arc::new(AtomicUsize::new(0)),
        serde_json::json!({"data": {"items": items}}).to_string(),
    )
    .await?;

    let job = dir.join("job.yaml");
    write_job(
        &job,
        &format!(
            r#"
env:
  job.name: http2ch-e2e
  parallelism: 1

source:
  Http:
    url: {api}/items
    method: GET
    format: json
    data-path: data.items
    columns: "id,name"

sink:
  ClickHouse:
    url: http://127.0.0.1:8123
    database: default
    table: {table}
    username: default
    password: ""
    primary-keys: id
    columns: "id,name"
    schema-save-mode: create_when_not_exist
"#
        ),
    );
    let log = dir.join("job.log");
    let child = start_job(&binary, &job, &dir, &log)?;
    wait_job(child, &log).await?;

    let count = ch_query(&format!("SELECT count() FROM default.`{table}`"))
        .await?
        .trim()
        .to_string();
    assert_eq!(count, "30", "job log:\n{}", log_tail(&log, 20));

    let _ = ch_exec(&format!("DROP TABLE IF EXISTS default.`{table}`")).await;
    Ok(())
}

#[tokio::test]
async fn rabbitmq_source_to_http_sink() -> anyhow::Result<()> {
    let binary = match (cli_binary(), wait_for(RABBITMQ, 1).await) {
        (Some(binary), true) => binary,
        _ => {
            eprintln!("SKIP: rabbitmq or CLI binary unreachable");
            return Ok(());
        }
    };
    let suffix = unique_suffix();
    let queue = format!("e2e_rmq2http_{suffix}");
    let dir = temp_path("rmq2http");

    // Publish 25 JSON-array messages into a fresh queue.
    let channel = rabbitmq_channel().await?;
    channel
        .queue_declare(
            queue.as_str().into(),
            lapin::options::QueueDeclareOptions {
                durable: true,
                ..Default::default()
            },
            lapin::types::FieldTable::default(),
        )
        .await?;
    for i in 0..25i64 {
        let payload = format!("[{i},\"u{i}\"]");
        let confirm = channel
            .basic_publish(
                "".into(),
                queue.as_str().into(),
                lapin::options::BasicPublishOptions::default(),
                payload.as_bytes(),
                lapin::BasicProperties::default(),
            )
            .await?;
        confirm.await?;
    }

    let received = Arc::new(AtomicUsize::new(0));
    let api = spawn_api(received.clone(), "{}".to_string()).await?;

    let job = dir.join("job.yaml");
    write_job(
        &job,
        &format!(
            r#"
env:
  job.name: rmq2http-e2e
  parallelism: 1

source:
  RabbitMQ:
    host: 127.0.0.1
    port: 5672
    username: guest
    password: guest
    queue-name: {queue}
    format: json

sink:
  Http:
    url: {api}/collect
    method: POST
"#
        ),
    );
    let log = dir.join("job.log");
    let child = start_job(&binary, &job, &dir, &log)?;

    // The RabbitMQ source is unbounded: wait until every message was
    // delivered, then stop the job.
    let mut delivered = 0usize;
    for _ in 0..120 {
        delivered = received.load(Ordering::SeqCst);
        if delivered >= 25 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
    kill_job(child).await;
    assert_eq!(delivered, 25, "job log:\n{}", log_tail(&log, 20));
    Ok(())
}
