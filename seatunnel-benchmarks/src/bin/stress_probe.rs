// End-to-end latency probe for the single-node stress test.
//
// Subscribes to the engine's Kafka output topics and, for every message,
// compares the row's embedded `ts_ms` (written by stress-gen on the host
// clock) with the probe's receive time. Messages with `ts_ms == 0` are
// snapshot/seed rows and are only counted, not timed.
//
// Exit policy: stop after `--idle-exit-sec` without messages once at least
// one load message was seen, or at `--max-runtime-sec` regardless. The final
// report is written as JSON to `--out`.

use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use rdkafka::consumer::{Consumer, StreamConsumer};
use rdkafka::Message;

fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

fn arg(name: &str) -> Option<String> {
    let args: Vec<String> = std::env::args().collect();
    args.iter()
        .position(|a| a == name)
        .and_then(|i| args.get(i + 1).cloned())
}

fn arg_or(name: &str, default: &str) -> String {
    arg(name).unwrap_or_else(|| default.to_string())
}

/// One recorded load-phase message.
struct Sample {
    /// 10-second arrival bucket since probe start (for the time series).
    bucket: u32,
    /// End-to-end latency in milliseconds.
    lat: i32,
    /// Topic index.
    topic: u8,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let bootstrap = arg_or("--bootstrap", "127.0.0.1:9092");
    let topics: Vec<String> = arg_or("--topics", "out")
        .split(',')
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .collect();
    let idle_exit = Duration::from_secs(arg_or("--idle-exit-sec", "90").parse::<u64>()?);
    let max_runtime = Duration::from_secs(arg_or("--max-runtime-sec", "2400").parse::<u64>()?);
    let report_out = arg_or("--out", "probe-report.json");
    let tag = arg_or("--tag", "run");
    let group = format!("stress-probe-{}-{}", tag, now_ms());

    let mut cfg = rdkafka::config::ClientConfig::new();
    cfg.set("bootstrap.servers", &bootstrap)
        .set("group.id", &group)
        .set("enable.auto.commit", "false")
        .set("enable.partition.eof", "false")
        .set("auto.offset.reset", "earliest")
        .set("session.timeout.ms", "30000");
    let consumer: StreamConsumer = cfg.create()?;
    let topic_refs: Vec<&str> = topics.iter().map(String::as_str).collect();
    consumer.subscribe(&topic_refs)?;

    eprintln!(
        "stress-probe[{}]: subscribed to {} (group {})",
        tag,
        topics.join(","),
        group
    );

    let start = Instant::now();
    let mut samples: Vec<Sample> = Vec::new();
    let mut snapshot_counts: Vec<u64> = vec![0; topics.len()];
    let mut last_message_at = Instant::now();
    let mut last_report = Instant::now();
    let mut window: Vec<i32> = Vec::new();
    let mut saw_load = false;

    loop {
        let elapsed = start.elapsed();
        if elapsed >= max_runtime {
            eprintln!("stress-probe[{}]: max runtime reached", tag);
            break;
        }
        if saw_load && last_message_at.elapsed() >= idle_exit {
            eprintln!("stress-probe[{}]: idle exit", tag);
            break;
        }
        match tokio::time::timeout(Duration::from_millis(500), consumer.recv()).await {
            Ok(Ok(msg)) => {
                last_message_at = Instant::now();
                let topic_idx = topics
                    .iter()
                    .position(|t| t == msg.topic())
                    .unwrap_or(usize::MAX);
                if topic_idx == usize::MAX {
                    continue;
                }
                let Some(payload) = msg.payload() else {
                    continue;
                };
                // Positional JSON array: [id, ts_ms, seq, payload].
                let value: serde_json::Value = match serde_json::from_slice(payload) {
                    Ok(v) => v,
                    Err(_) => {
                        eprintln!(
                            "stress-probe[{}]: non-JSON payload on {}",
                            tag,
                            topics[topic_idx]
                        );
                        continue;
                    }
                };
                let ts = value
                    .as_array()
                    .and_then(|a| a.get(1))
                    .and_then(|v| v.as_i64())
                    .unwrap_or(-1);
                if ts < 0 {
                    continue;
                }
                if ts == 0 {
                    snapshot_counts[topic_idx] += 1;
                } else {
                    saw_load = true;
                    let lat = (now_ms() - ts).max(0) as i32;
                    window.push(lat);
                    samples.push(Sample {
                        bucket: (start.elapsed().as_secs() / 10) as u32,
                        lat,
                        topic: topic_idx as u8,
                    });
                }
            }
            Ok(Err(e)) => {
                eprintln!("stress-probe[{}]: consumer error: {}", tag, e);
            }
            Err(_) => { /* poll timeout — loop re-checks exit conditions */ }
        }
        if last_report.elapsed() >= Duration::from_secs(5) {
            last_report = Instant::now();
            let (count, avg, max) = if window.is_empty() {
                (0, 0.0, 0)
            } else {
                let sum: i64 = window.iter().map(|&x| x as i64).sum();
                (
                    window.len(),
                    sum as f64 / window.len() as f64,
                    *window.iter().max().unwrap(),
                )
            };
            eprintln!(
                "stress-probe[{}] PROGRESS t={:5}s total_load={} window(5s): n={} avg={:.1}ms max={}ms snapshot={:?}",
                tag,
                start.elapsed().as_secs(),
                samples.len(),
                count,
                avg,
                max,
                snapshot_counts
            );
            window.clear();
        }
    }

    // ------------------------------------------------------------------
    // Final report.
    // ------------------------------------------------------------------
    fn percentile(sorted: &[i32], p: f64) -> i32 {
        if sorted.is_empty() {
            return 0;
        }
        let idx = ((sorted.len() as f64 - 1.0) * p).round() as usize;
        sorted[idx.min(sorted.len() - 1)]
    }

    let mut report = serde_json::json!({
        "tag": tag,
        "topics": topics,
        "total_load_messages": samples.len(),
        "snapshot_counts": snapshot_counts,
        "elapsed_sec": start.elapsed().as_secs_f64(),
    });

    let mut per_topic = serde_json::Map::new();
    for (idx, topic) in topics.iter().enumerate() {
        let mut lats: Vec<i32> = samples
            .iter()
            .filter(|s| s.topic as usize == idx)
            .map(|s| s.lat)
            .collect();
        lats.sort_unstable();
        let count = lats.len();
        let sum: i64 = lats.iter().map(|&x| x as i64).sum();
        per_topic.insert(
            topic.clone(),
            serde_json::json!({
                "load_count": count,
                "snapshot_count": snapshot_counts[idx],
                "avg_ms": if count == 0 { 0.0 } else { sum as f64 / count as f64 },
                "p50_ms": percentile(&lats, 0.50),
                "p90_ms": percentile(&lats, 0.90),
                "p95_ms": percentile(&lats, 0.95),
                "p99_ms": percentile(&lats, 0.99),
                "p999_ms": percentile(&lats, 0.999),
                "max_ms": lats.last().copied().unwrap_or(0),
            }),
        );
    }
    report["per_topic"] = serde_json::Value::Object(per_topic);

    let mut all: Vec<i32> = samples.iter().map(|s| s.lat).collect();
    all.sort_unstable();
    let sum: i64 = all.iter().map(|&x| x as i64).sum();
    report["overall"] = serde_json::json!({
        "load_count": all.len(),
        "avg_ms": if all.is_empty() { 0.0 } else { sum as f64 / all.len() as f64 },
        "p50_ms": percentile(&all, 0.50),
        "p90_ms": percentile(&all, 0.90),
        "p95_ms": percentile(&all, 0.95),
        "p99_ms": percentile(&all, 0.99),
        "p999_ms": percentile(&all, 0.999),
        "max_ms": all.last().copied().unwrap_or(0),
    });

    // 10-second bucket time series (p99 per bucket, all topics combined).
    let max_bucket = samples.iter().map(|s| s.bucket).max().unwrap_or(0);
    let mut series = Vec::new();
    for b in 0..=max_bucket {
        let mut lats: Vec<i32> = samples
            .iter()
            .filter(|s| s.bucket == b)
            .map(|s| s.lat)
            .collect();
        if lats.is_empty() {
            continue;
        }
        lats.sort_unstable();
        let sum: i64 = lats.iter().map(|&x| x as i64).sum();
        series.push(serde_json::json!({
            "t_sec": b * 10,
            "count": lats.len(),
            "avg_ms": sum as f64 / lats.len() as f64,
            "p99_ms": percentile(&lats, 0.99),
            "max_ms": lats.last().copied().unwrap_or(0),
        }));
    }
    report["series_10s"] = serde_json::Value::Array(series);

    let pretty = serde_json::to_string_pretty(&report)?;
    std::fs::write(&report_out, &pretty)?;
    println!("{}", pretty);
    Ok(())
}
