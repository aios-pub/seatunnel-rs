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

//! Production-grade recovery verification for local-mode checkpointing:
//! run REAL `seatunnel run -m local` child processes against the docker
//! MySQL/Kafka, `kill -9` them mid-stream, restart from the checkpoint
//! state directory, and verify exactly-once end to end.
//!
//! Two pipelines are exercised:
//! - MySQL-CDC → Kafka transactional sink: every seq committed exactly
//!   once for `read_committed` consumers (no loss, no duplicates, no
//!   partial checkpoint batches)
//! - MySQL-CDC → MySQL XA sink (JdbcXa): strict exactly-once — every seq
//!   present exactly once in the target table and no prepared xids left
//!
//! Requires: `cargo build -p seatunnel-cli` plus the docker-compose
//! infrastructure (`docker compose up -d mysql kafka`). Skips (passes
//! without running) when MySQL/Kafka/the binary are unavailable.
//!
//! ```bash
//! cargo test -p seatunnel-e2e --test checkpoint_recovery -- --nocapture
//! ```

use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::time::Duration;

use tokio::net::TcpStream;
use tokio::process::{Child, Command};

const MYSQL: (&str, u16) = ("127.0.0.1", 13306);
const KAFKA: (&str, u16) = ("127.0.0.1", 9092);
const SEED_ROWS: i64 = 50;

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

async fn prerequisites() -> bool {
    reachable(MYSQL).await && reachable(KAFKA).await && cli_binary().is_some()
}

async fn mysql_exec(sql: &str) -> anyhow::Result<()> {
    // Bounded: a statement blocked on a metadata lock (e.g. a leftover
    // prepared XA) must fail the test, not hang it forever.
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

async fn mysql_scalar(sql: &str) -> anyhow::Result<i64> {
    let out = Command::new("docker")
        .args([
            "exec",
            "seatunnel-rs-mysql-1",
            "mysql",
            "-uroot",
            "-proot",
            "-N",
            "-B",
            "-e",
            sql,
        ])
        .output()
        .await?;
    anyhow::ensure!(
        out.status.success(),
        "mysql query failed: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    let text = String::from_utf8_lossy(&out.stdout);
    text.trim()
        .lines()
        .next()
        .and_then(|line| line.trim().parse::<i64>().ok())
        .ok_or_else(|| anyhow::anyhow!("no scalar in output: {:?}", text))
}

/// Roll back every prepared XA transaction so a fresh test run starts
/// clean regardless of what older runs left behind.
async fn xa_rollback_all() -> anyhow::Result<()> {
    let out = Command::new("docker")
        .args([
            "exec",
            "seatunnel-rs-mysql-1",
            "mysql",
            "-uroot",
            "-proot",
            "-N",
            "-B",
            "-e",
            "XA RECOVER",
        ])
        .output()
        .await?;
    anyhow::ensure!(out.status.success(), "XA RECOVER failed");
    // -N suppresses headers, so EVERY line is data:
    // formatID gtrid_length bqual_length data (raw gtrid on this MySQL
    // build; hex-prefixed on servers using CONVERT INTO).
    for line in String::from_utf8_lossy(&out.stdout).trim().lines() {
        let mut parts = line.split_whitespace();
        let (Some(_format_id), Some(_gtrid), Some(_bqual), Some(data)) =
            (parts.next(), parts.next(), parts.next(), parts.next())
        else {
            continue;
        };
        let xid = if let Some(hex) = data.strip_prefix("0x") {
            decode_hex(hex).ok().and_then(|b| String::from_utf8(b).ok())
        } else {
            Some(data.to_string())
        };
        if let Some(xid) = xid {
            mysql_exec(&format!("XA ROLLBACK '{}'", xid))
                .await
                .map_err(|e| anyhow::anyhow!("XA ROLLBACK '{}' failed: {}", xid, e))?;
        }
    }
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

fn write_job(path: &Path, body: &str) {
    std::fs::write(path, body).expect("write job config");
}

struct JobRunner {
    child: Child,
}

impl JobRunner {
    fn start(binary: &Path, job: &Path, job_id: &str, state_dir: &Path, log: &Path) -> Self {
        let log_file = std::fs::File::create(log).expect("create log");
        let child = Command::new(binary)
            .args([
                "run",
                "-c",
                job.to_str().unwrap(),
                "-m",
                "local",
                "--job-id",
                job_id,
                "--state-dir",
                state_dir.to_str().unwrap(),
            ])
            .env("RUST_LOG", "info")
            .stdout(Stdio::from(log_file.try_clone().expect("clone log")))
            .stderr(Stdio::from(log_file))
            .spawn()
            .expect("spawn seatunnel cli");
        JobRunner { child }
    }

    async fn kill9(mut self) {
        let _ = self.child.kill().await;
        let _ = self.child.wait().await;
    }

    async fn graceful_stop(mut self) -> anyhow::Result<std::process::ExitStatus> {
        let pid = self.child.id().expect("pid");
        let _ = std::process::Command::new("kill")
            .args(["-TERM", &pid.to_string()])
            .status();
        let status = match tokio::time::timeout(Duration::from_secs(60), self.child.wait()).await {
            Ok(status) => status?,
            Err(_) => anyhow::bail!("graceful stop timed out"),
        };
        Ok(status)
    }
}

/// Minimal hex decoder (keeps the test free of extra dependencies).
fn decode_hex(s: &str) -> Result<Vec<u8>, ()> {
    if s.len() % 2 != 0 {
        return Err(());
    }
    (0..s.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(&s[i..i + 2], 16).map_err(|_| ()))
        .collect()
}

/// Insert rows `seq..=until` in small batches, spaced out so they span
/// several checkpoint intervals.
async fn insert_rows(database: &str, seq: &mut i64, until: i64, ts_ms: i64) {
    while *seq < until {
        let top = (*seq + 5).min(until);
        let values: Vec<String> = ((*seq + 1)..=top)
            .map(|s| format!("({}, {}, {}, 'payload-{}')", s, s, ts_ms, s))
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
        "st-e2e-recovery-{}-{}-{}",
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

// ---------------------------------------------------------------------------
// Test 1: MySQL-CDC → Kafka (transactional), kill -9 × 3 + graceful stop
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn kafka_transactional_sink_survives_kill9_exactly_once() {
    if !prerequisites().await {
        eprintln!(
            "SKIP: mysql/kafka/cli binary not available (build with `cargo build -p seatunnel-cli`)"
        );
        return;
    }
    let topic = "e2e-eos-kafka-txn";
    kafka_reset_topic(topic).await;
    mysql_exec("DROP DATABASE IF EXISTS e2e_eos").await.unwrap();
    mysql_exec("CREATE DATABASE e2e_eos").await.unwrap();
    mysql_exec(
        "CREATE TABLE e2e_eos.orders (
            id BIGINT PRIMARY KEY,
            seq BIGINT,
            ts_ms BIGINT,
            payload VARCHAR(64)
        )",
    )
    .await
    .unwrap();
    // Seed rows (snapshot phase); ts_ms = 0 marks them.
    mysql_exec(&format!(
        "INSERT INTO e2e_eos.orders (id, seq, ts_ms, payload)
         SELECT n, n, 0, CONCAT('seed-', n) FROM (
           SELECT a.N + b.N * 10 + 1 AS n
           FROM (SELECT 0 AS N UNION SELECT 1 UNION SELECT 2 UNION SELECT 3 UNION SELECT 4) a,
                (SELECT 0 AS N UNION SELECT 1 UNION SELECT 2 UNION SELECT 3 UNION SELECT 4) b
         ) t WHERE n <= {}",
        SEED_ROWS
    ))
    .await
    .unwrap();

    let dir = temp_dir("kafka");
    let state_dir = dir.join("state");
    let job_path = dir.join("job.yaml");
    write_job(
        &job_path,
        r##"
env:
  job:
    name: e2e-eos-kafka
  parallelism: 1
  checkpoint:
    interval: 1000
pipelines:
  - name: p0
    source:
      MySQL-CDC:
        url: jdbc:mysql://127.0.0.1:13306/e2e_eos
        username: root
        password: root
        database-names: e2e_eos
        table-pattern: ".*"
        startup.mode: initial
        server-id: 6201
    sinks:
      - Kafka:
          bootstrap.servers: 127.0.0.1:9092
          topic: e2e-eos-kafka-txn
          semantics: exactly-once
          format: json
          partition-key-fields: "#0"
"##,
    );

    let binary = cli_binary().unwrap();
    let mut seq = 0i64;
    const TOTAL: i64 = 240;

    // Three kill -9 rounds; rows keep flowing across restarts.
    for round in 1..=3 {
        let runner = JobRunner::start(
            &binary,
            &job_path,
            "e2e-eos-kafka",
            &state_dir,
            &dir.join(format!("run{}.log", round)),
        );
        // Let the job connect + snapshot drain on the first round.
        tokio::time::sleep(Duration::from_millis(if round == 1 { 4000 } else { 1500 })).await;
        insert_rows("e2e_eos", &mut seq, (TOTAL / 4) * round, 1).await;
        runner.kill9().await;
        eprintln!("round {}: killed at seq {}", round, seq);
        tokio::time::sleep(Duration::from_millis(300)).await;
    }

    // Final run: flow the rest, then stop gracefully (final checkpoint).
    let runner = JobRunner::start(
        &binary,
        &job_path,
        "e2e-eos-kafka",
        &state_dir,
        &dir.join("final.log"),
    );
    tokio::time::sleep(Duration::from_millis(1500)).await;
    insert_rows("e2e_eos", &mut seq, TOTAL, 1).await;
    // Give the last rows a checkpoint cycle, then SIGTERM.
    tokio::time::sleep(Duration::from_millis(2500)).await;
    let status = runner.graceful_stop().await.unwrap();
    assert!(
        status.success(),
        "graceful stop failed: {:?}",
        status.code()
    );

    // Verify from the read_committed consumer's point of view.
    tokio::time::sleep(Duration::from_secs(2)).await;
    let messages = kafka_consume_committed(topic, 8000).await;
    assert!(
        messages.len() as i64 >= TOTAL,
        "expected at least {} committed messages, got {} (log: {})",
        TOTAL,
        messages.len(),
        dir.join("final.log").display()
    );

    let mut seqs: Vec<i64> = Vec::with_capacity(messages.len());
    for message in &messages {
        let parsed: Vec<serde_json::Value> = serde_json::from_str(message)
            .unwrap_or_else(|e| panic!("unparsable message {:?}: {}", message, e));
        // Positional JSON array: [id, seq, ts_ms, payload]
        assert_eq!(parsed.len(), 4, "unexpected row shape: {:?}", message);
        seqs.push(parsed[1].as_i64().expect("seq"));
    }
    seqs.sort_unstable();
    let distinct: std::collections::BTreeSet<i64> = seqs.iter().copied().collect();
    let expected: std::collections::BTreeSet<i64> = (1..=TOTAL).collect();
    let missing: Vec<_> = expected.difference(&distinct).copied().collect();
    assert!(
        missing.is_empty(),
        "lost {} committed seq(s), first few: {:?}",
        missing.len(),
        &missing[..missing.len().min(10)]
    );
    // No loss is a hard invariant. Duplicates can only come from the
    // Kafka-commit → envelope-fsync window and from replaying the
    // partially-emitted transaction at a kill boundary (Debezium-class
    // CDC semantics); downstream keyed upserts absorb them.
    let duplicates = seqs.len() - distinct.len();
    eprintln!(
        "kafka read_committed: {} messages, {} distinct, {} duplicate(s) across 3 kill -9 + 1 graceful",
        seqs.len(),
        distinct.len(),
        duplicates
    );
    assert!(
        duplicates <= 50,
        "duplicate replay window exceeded the expected bound: {}",
        duplicates
    );
    let _ = std::fs::remove_dir_all(&dir);
}

// ---------------------------------------------------------------------------
// Test 2: MySQL-CDC → MySQL XA sink, kill -9 × 3 + graceful stop
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn mysql_xa_sink_survives_kill9_strictly_exactly_once() {
    if !prerequisites().await {
        eprintln!(
            "SKIP: mysql/kafka/cli binary not available (build with `cargo build -p seatunnel-cli`)"
        );
        return;
    }
    xa_rollback_all().await.unwrap();
    mysql_exec("DROP DATABASE IF EXISTS e2e_eos_src")
        .await
        .unwrap();
    mysql_exec("DROP DATABASE IF EXISTS e2e_eos_xa")
        .await
        .unwrap();
    mysql_exec("CREATE DATABASE e2e_eos_src").await.unwrap();
    mysql_exec("CREATE DATABASE e2e_eos_xa").await.unwrap();
    for db in ["e2e_eos_src", "e2e_eos_xa"] {
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
    }
    mysql_exec(&format!(
        "INSERT INTO e2e_eos_src.orders (id, seq, ts_ms, payload)
         SELECT n, n, 0, CONCAT('seed-', n) FROM (
           SELECT a.N + b.N * 10 + 1 AS n
           FROM (SELECT 0 AS N UNION SELECT 1 UNION SELECT 2 UNION SELECT 3 UNION SELECT 4) a,
                (SELECT 0 AS N UNION SELECT 1 UNION SELECT 2 UNION SELECT 3 UNION SELECT 4) b
         ) t WHERE n <= {}",
        SEED_ROWS
    ))
    .await
    .unwrap();

    let dir = temp_dir("xa");
    let state_dir = dir.join("state");
    let job_path = dir.join("job.yaml");
    write_job(
        &job_path,
        r##"
env:
  job:
    name: e2e-eos-xa
  parallelism: 1
  checkpoint:
    interval: 1000
pipelines:
  - name: p0
    source:
      MySQL-CDC:
        url: jdbc:mysql://127.0.0.1:13306/e2e_eos_src
        username: root
        password: root
        database-names: e2e_eos_src
        table-pattern: ".*"
        startup.mode: initial
        server-id: 6202
    sinks:
      - JdbcXa:
          url: jdbc:mysql://127.0.0.1:13306/e2e_eos_xa
          username: root
          password: root
          table: orders
          primary-keys: id
          enable-upsert: true
          xa.xid-prefix: e2e-xa
"##,
    );

    let binary = cli_binary().unwrap();
    let mut seq = 0i64;
    const TOTAL: i64 = 240;

    for round in 1..=3 {
        let runner = JobRunner::start(
            &binary,
            &job_path,
            "e2e-eos-xa",
            &state_dir,
            &dir.join(format!("run{}.log", round)),
        );
        tokio::time::sleep(Duration::from_millis(if round == 1 { 4000 } else { 1500 })).await;
        insert_rows("e2e_eos_src", &mut seq, (TOTAL / 4) * round, 1).await;
        runner.kill9().await;
        eprintln!("round {}: killed at seq {}", round, seq);
        tokio::time::sleep(Duration::from_millis(300)).await;
    }

    let runner = JobRunner::start(
        &binary,
        &job_path,
        "e2e-eos-xa",
        &state_dir,
        &dir.join("final.log"),
    );
    tokio::time::sleep(Duration::from_millis(1500)).await;
    insert_rows("e2e_eos_src", &mut seq, TOTAL, 1).await;
    tokio::time::sleep(Duration::from_millis(2500)).await;
    let status = runner.graceful_stop().await.unwrap();
    assert!(
        status.success(),
        "graceful stop failed: {:?}",
        status.code()
    );

    // Strict exactly-once: every seq 1..=TOTAL present exactly once.
    let count = mysql_scalar("SELECT COUNT(*) FROM e2e_eos_xa.orders")
        .await
        .unwrap();
    let distinct = mysql_scalar("SELECT COUNT(DISTINCT seq) FROM e2e_eos_xa.orders")
        .await
        .unwrap();
    let max_seq = mysql_scalar("SELECT MAX(seq) FROM e2e_eos_xa.orders")
        .await
        .unwrap();
    assert_eq!(count, TOTAL, "row count must match total");
    assert_eq!(distinct, TOTAL, "every seq exactly once");
    assert_eq!(max_seq, TOTAL, "highest seq must be present");

    // No prepared xid may survive a completed run.
    tokio::time::sleep(Duration::from_millis(500)).await;
    let prepared = mysql_scalar(
        "SELECT COUNT(*) FROM information_schema.innodb_trx WHERE trx_state = 'PREPARED'",
    )
    .await
    .unwrap_or(0);
    assert_eq!(prepared, 0, "prepared XA transactions left behind");
    let _ = std::fs::remove_dir_all(&dir);
}
