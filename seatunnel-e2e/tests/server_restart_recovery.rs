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

//! Server-restart recovery verification against the docker MySQL/Kafka:
//! a REAL hybrid engine-server process runs a MySQL-CDC → Kafka job,
//! gets `kill -9`'d, MySQL keeps receiving writes while it is down, and
//! the server is restarted with the SAME state dir. The job must resume
//! WITHOUT any manual resubmission (register-time lost-task
//! reconciliation) and every row — including the ones committed to MySQL
//! during the downtime — must reach Kafka (at-least-once; bounded
//! duplicates for read_committed consumers).
//!
//! This is the full-persistence path the in-process integration test
//! models: the restarted process reloads its coordinator from the
//! on-disk Raft state under <state-dir>/raft and its worker resumes the
//! task from the on-disk checkpoint store.
//!
//! Requires: `cargo build -p seatunnel-engine-server` plus the
//! docker-compose infrastructure (`docker compose up -d mysql kafka`)
//! and a free port 5800. Skips (passes without running) when
//! MySQL/Kafka/the binary are unavailable.
//!
//! ```bash
//! cargo test -p seatunnel-e2e --test server_restart_recovery -- --nocapture
//! ```

use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::time::Duration;

use tokio::net::TcpStream;
use tokio::process::{Child, Command};

use seatunnel_engine_client::EngineClient;

const MYSQL: (&str, u16) = ("127.0.0.1", 13306);
const KAFKA: (&str, u16) = ("127.0.0.1", 9092);
const MASTER: (&str, u16) = ("127.0.0.1", 5800);
const SEED_ROWS: i64 = 40;
const TOTAL: i64 = 240;
/// Rows committed to MySQL while the engine is DOWN — the critical
/// continuity window the CDC source must replay from its binlog
/// position after the restart.
const DOWN_FROM: i64 = 121;
const DOWN_UNTIL: i64 = 180;

fn server_binary() -> Option<PathBuf> {
    // Manifest is <repo>/seatunnel-e2e/tests → binary at <repo>/target/debug.
    let mut dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    dir.pop(); // seatunnel-e2e
    let bin = dir.join("target/debug/seatunnel-engine-server");
    bin.exists().then_some(bin)
}

async fn reachable(addr: (&str, u16)) -> bool {
    TcpStream::connect(addr).await.is_ok()
}

async fn prerequisites() -> bool {
    reachable(MYSQL).await
        && reachable(KAFKA).await
        && server_binary().is_some()
        && !reachable(MASTER).await // port must be free for our server
}

async fn mysql_exec(sql: &str) -> anyhow::Result<()> {
    let out = tokio::time::timeout(
        Duration::from_secs(30),
        Command::new("docker")
            .args([
                "exec",
                "seatunnel-rs-mysql-1",
                "mysql",
                "-uroot",
                "-proot",
                "-e",
                sql,
            ])
            .output(),
    )
    .await
    .map_err(|_| anyhow::anyhow!("mysql exec timed out: {}", sql))??;
    anyhow::ensure!(
        out.status.success(),
        "mysql exec failed: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    Ok(())
}

async fn kafka_reset_topic(topic: &str) {
    let _ = Command::new("docker")
        .args([
            "exec",
            "seatunnel-rs-kafka-1",
            "kafka-topics",
            "--bootstrap-server",
            "localhost:9092",
            "--delete",
            "--topic",
            topic,
        ])
        .output()
        .await;
    tokio::time::sleep(Duration::from_millis(500)).await;
    let _ = Command::new("docker")
        .args([
            "exec",
            "seatunnel-rs-kafka-1",
            "kafka-topics",
            "--bootstrap-server",
            "localhost:9092",
            "--create",
            "--if-not-exists",
            "--topic",
            topic,
            "--partitions",
            "1",
            "--replication-factor",
            "1",
        ])
        .output()
        .await;
}

/// Consume the topic with `read_committed` from the beginning.
async fn kafka_consume_committed(topic: &str, timeout_ms: u32) -> Vec<String> {
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
            "--isolation-level",
            "read_committed",
            "--timeout-ms",
            &timeout_ms.to_string(),
        ])
        .output()
        .await;
    let stdout = match out {
        Ok(out) => String::from_utf8_lossy(&out.stdout).to_string(),
        Err(_) => String::new(),
    };
    stdout
        .lines()
        .map(str::trim)
        .filter(|l| !l.is_empty())
        .map(str::to_string)
        .collect()
}

/// Distinct `seq` values present in the topic (positional JSON rows:
/// [id, seq, ts_ms, payload]).
async fn kafka_distinct_seqs(topic: &str, timeout_ms: u32) -> std::collections::BTreeSet<i64> {
    kafka_consume_committed(topic, timeout_ms)
        .await
        .iter()
        .filter_map(|message| {
            serde_json::from_str::<Vec<serde_json::Value>>(message)
                .ok()
                .and_then(|parsed| parsed.get(1).and_then(|v| v.as_i64()))
        })
        .collect()
}

struct Server {
    child: Child,
}

impl Server {
    /// Hybrid single node against the fixed master port; the SAME
    /// state_dir across runs is the whole point (durable Raft state +
    /// worker-local checkpoints live there).
    fn start(binary: &Path, state_dir: &Path, log: &Path) -> Self {
        let log_file = std::fs::File::create(log).expect("create log");
        let child = Command::new(binary)
            .args([
                "--role",
                "hybrid",
                "--addr",
                &format!("127.0.0.1:{}", MASTER.1),
                "--state-dir",
                state_dir.to_str().unwrap(),
            ])
            .env("RUST_LOG", "info")
            .stdout(Stdio::from(log_file.try_clone().expect("clone log")))
            .stderr(Stdio::from(log_file))
            .spawn()
            .expect("spawn seatunnel-engine-server");
        Server { child }
    }

    async fn kill9(mut self) {
        let _ = self.child.kill().await;
        let _ = self.child.wait().await;
    }
}

async fn wait_for_cluster(
    client: &EngineClient,
    deadline_s: u64,
    log: &Path,
) -> anyhow::Result<()> {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(deadline_s);
    loop {
        if let Ok(info) = client.get_cluster_info().await
            && info.available_workers >= 1
        {
            return Ok(());
        }
        anyhow::ensure!(
            tokio::time::Instant::now() < deadline,
            "engine-server did not accept a registered worker in {}s (log: {})",
            deadline_s,
            log.display()
        );
        tokio::time::sleep(Duration::from_millis(300)).await;
    }
}

/// Insert rows `seq..=until` in small batches, spaced out so they span
/// several checkpoint intervals.
async fn insert_rows(database: &str, seq: &mut i64, until: i64) {
    while *seq < until {
        let top = (*seq + 5).min(until);
        let values: Vec<String> = ((*seq + 1)..=top)
            .map(|s| format!("({}, {}, 1, 'payload-{}')", s, s, s))
            .collect();
        let _ = mysql_exec(&format!(
            "INSERT INTO {}.orders (id, seq, ts_ms, payload) VALUES {}",
            database,
            values.join(",")
        ))
        .await;
        *seq = top;
        tokio::time::sleep(Duration::from_millis(120)).await;
    }
}

fn temp_dir(tag: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!(
        "st-e2e-server-restart-{}-{}-{}",
        tag,
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis()
    ));
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn hybrid_server_restart_recovers_running_cdc_job() {
    if !prerequisites().await {
        eprintln!(
            "SKIP: mysql/kafka/engine-server binary not available or port 5800 busy \
             (build with `cargo build -p seatunnel-engine-server`)"
        );
        return;
    }
    let topic = "e2e-server-restart";
    let db = "e2e_srv_restart";
    let job_id = "e2e-srv-restart";
    kafka_reset_topic(topic).await;
    mysql_exec(&format!("DROP DATABASE IF EXISTS {}", db))
        .await
        .unwrap();
    mysql_exec(&format!("CREATE DATABASE {}", db))
        .await
        .unwrap();
    mysql_exec(&format!(
        "CREATE TABLE {}.orders (
            id BIGINT PRIMARY KEY,
            seq BIGINT,
            ts_ms BIGINT,
            payload VARCHAR(64)
        )",
        db
    ))
    .await
    .unwrap();
    // Seed CONTIGUOUS ids 1..=SEED_ROWS (a sparse generator would leave
    // gaps the snapshot cannot fill; seq == id keeps accounting simple).
    let mut seq = 0i64;
    insert_rows(db, &mut seq, SEED_ROWS).await;
    assert_eq!(seq, SEED_ROWS, "seeding must be contiguous");

    let dir = temp_dir("main");
    let state_dir = dir.join("state");
    let binary = server_binary().unwrap();
    // The gRPC SubmitJob path takes JSON config bytes (the CLI converts
    // YAML before sending); same `pipelines:` schema the local mode uses.
    let config = serde_json::json!({
        "env": {
            "job": { "name": "e2e-server-restart" },
            "parallelism": 1,
            "checkpoint": { "interval": 5000 }
        },
        "pipelines": [
            {
                "name": "p0",
                "source": {
                    "MySQL-CDC": {
                        "url": "jdbc:mysql://127.0.0.1:13306/e2e_srv_restart",
                        "username": "root",
                        "password": "root",
                        "database-names": ["e2e_srv_restart"],
                        "table-pattern": ".*",
                        "startup.mode": "initial",
                        "server-id": 6301
                    }
                },
                "sinks": [
                    {
                        "Kafka": {
                            "bootstrap.servers": "127.0.0.1:9092",
                            "topic": "e2e-server-restart",
                            "semantics": "exactly-once",
                            "format": "json",
                            "partition-key-fields": "#0"
                        }
                    }
                ]
            }
        ]
    });
    let config_bytes = serde_json::to_vec(&config).unwrap();

    let client = EngineClient::new(&format!("127.0.0.1:{}", MASTER.1));

    // --- Round 1: submit, snapshot drains, steady-state rows flow.
    let server = Server::start(&binary, &state_dir, &dir.join("run1.log"));
    wait_for_cluster(&client, 30, &dir.join("run1.log"))
        .await
        .unwrap();
    let resp = client
        .submit_job(job_id, "e2e-server-restart", config_bytes, 0)
        .await
        .unwrap();
    assert!(resp.success, "submit failed: {}", resp.message);

    // Snapshot phase: the seed rows must arrive (they only exist in
    // MySQL, so seeing them in Kafka proves the pipeline runs).
    let deadline = tokio::time::Instant::now() + Duration::from_secs(60);
    loop {
        let seen = kafka_distinct_seqs(topic, 3000).await;
        if seen.len() as i64 >= SEED_ROWS {
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "snapshot rows never reached kafka ({}/{}; log: {})",
            seen.len(),
            SEED_ROWS,
            dir.join("run1.log").display()
        );
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
    eprintln!("round 1: snapshot drained ({} seed rows)", SEED_ROWS);

    insert_rows(db, &mut seq, DOWN_FROM - 1).await;
    let deadline = tokio::time::Instant::now() + Duration::from_secs(60);
    loop {
        let seen = kafka_distinct_seqs(topic, 3000).await;
        if seen.len() as i64 >= DOWN_FROM - 1 {
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "steady-state rows never reached kafka ({}/{}; log: {})",
            seen.len(),
            DOWN_FROM - 1,
            dir.join("run1.log").display()
        );
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
    eprintln!("round 1: {} rows flowing, killing -9", seq);

    // --- The restart under test.
    server.kill9().await;
    tokio::time::sleep(Duration::from_millis(300)).await;

    // MySQL keeps receiving commits while the engine is DOWN.
    insert_rows(db, &mut seq, DOWN_UNTIL).await;
    eprintln!(
        "engine down: rows {}..={} committed to MySQL only",
        DOWN_FROM, seq
    );

    // --- Round 2: same state dir, same default worker id. The job must
    // resume WITHOUT resubmission (register-time reconciliation).
    let server = Server::start(&binary, &state_dir, &dir.join("run2.log"));
    wait_for_cluster(&client, 30, &dir.join("run2.log"))
        .await
        .unwrap();

    // The historical job is still there, still non-terminal.
    let status = client
        .get_job_status(job_id)
        .await
        .expect("job status after restart");
    assert_eq!(status.state, 3, "job must still be RUNNING after reload");
    eprintln!(
        "round 2: job '{}' present (state RUNNING), awaiting recovery",
        job_id
    );

    // Marker rows: if recovery were broken (zombie Running task), these
    // and the downtime rows would NEVER appear and this loop times out.
    insert_rows(db, &mut seq, TOTAL).await;

    let deadline = tokio::time::Instant::now() + Duration::from_secs(120);
    loop {
        let seen = kafka_distinct_seqs(topic, 5000).await;
        if seen.len() as i64 >= TOTAL {
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "job did not resume after restart: {} / {} rows in kafka; downtime rows missing: {:?}; \
             log: {}",
            seen.len(),
            TOTAL,
            (DOWN_FROM..=TOTAL)
                .filter(|s| !seen.contains(s))
                .take(5)
                .collect::<Vec<_>>(),
            dir.join("run2.log").display()
        );
        tokio::time::sleep(Duration::from_millis(1000)).await;
    }

    // The recovery machinery must have fired (not a silent cold path).
    let run2_log = std::fs::read_to_string(dir.join("run2.log")).unwrap_or_default();
    assert!(
        run2_log.contains("Restart recovery:"),
        "run2 log must show the restart-recovery reconciliation, got:\n{}",
        &run2_log[run2_log.len().saturating_sub(2000)..]
    );

    // Final accounting: no loss (hard invariant), bounded duplicates
    // (replay window around the kill).
    let messages = kafka_consume_committed(topic, 10_000).await;
    let mut seqs: Vec<i64> = Vec::with_capacity(messages.len());
    for message in &messages {
        let parsed: Vec<serde_json::Value> = serde_json::from_str(message)
            .unwrap_or_else(|e| panic!("unparsable message {:?}: {}", message, e));
        seqs.push(parsed[1].as_i64().expect("seq"));
    }
    let distinct: std::collections::BTreeSet<i64> = seqs.iter().copied().collect();
    let expected: std::collections::BTreeSet<i64> = (1..=TOTAL).collect();
    let missing: Vec<_> = expected.difference(&distinct).copied().collect();
    assert!(
        missing.is_empty(),
        "lost {} seq(s) across the server restart, first few: {:?}",
        missing.len(),
        &missing[..missing.len().min(10)]
    );
    let duplicates = seqs.len() - distinct.len();
    eprintln!(
        "kafka read_committed: {} messages, {} distinct, {} duplicate(s) across kill -9 restart",
        seqs.len(),
        distinct.len(),
        duplicates
    );
    assert!(
        duplicates <= 30,
        "duplicate replay window exceeded the expected bound: {}",
        duplicates
    );

    server.kill9().await;
    let _ = std::fs::remove_dir_all(&dir);
}
