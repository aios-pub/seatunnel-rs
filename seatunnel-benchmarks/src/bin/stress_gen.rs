// Load generator for the single-node multi-source/multi-sink stress test.
//
// Inserts timestamped rows round-robin across N databases x N tables at a
// target rows-per-second rate. Every generated row carries `ts_ms` set to
// the generator's wall clock, which the latency probe compares against its
// own receive time to derive end-to-end latency. Seeded (snapshot) rows use
// `ts_ms = 0` and are excluded from latency statistics by the probe.

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use mysql_async::prelude::*;

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

const ALPHANUM: &[u8] = b"abcdefghijklmnopqrstuvwxyz0123456789";

fn payload(seq: u64) -> String {
    // Deterministic, comma/quote-free so it can be inlined in SQL.
    let mut s = format!("payload-{seq:08}-");
    for i in 0..24 {
        s.push(ALPHANUM[((seq as usize) + i * 7) % ALPHANUM.len()] as char);
    }
    s
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let url = arg_or("--url", "mysql://root:root@127.0.0.1:13306");
    let databases: Vec<String> = arg_or("--databases", "perf_a")
        .split(',')
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .collect();
    let tables: u64 = arg_or("--tables", "250").parse()?;
    let rate: f64 = arg_or("--rate", "2000").parse()?;
    let duration = Duration::from_secs(arg_or("--duration-sec", "120").parse::<u64>()?);
    let workers: usize = arg_or("--workers", "8").parse()?;
    let batch: u64 = arg_or("--batch", "1").parse()?;
    let out_file = arg_or("--out", "");

    if databases.is_empty() {
        anyhow::bail!("--databases must list at least one database");
    }

    eprintln!(
        "stress-gen: {} databases x {} tables, rate={} rows/s, duration={:?}, workers={}, batch={}",
        databases.len(),
        tables,
        rate,
        duration,
        workers,
        batch
    );

    let opts = mysql_async::Opts::from_url(&url)?;
    let pool = mysql_async::Pool::new(opts);

    let table_cursor = Arc::new(AtomicU64::new(0));
    let seq = Arc::new(AtomicU64::new(0));
    let inserted = Arc::new(AtomicU64::new(0));
    let failed = Arc::new(AtomicU64::new(0));
    let stop = Arc::new(AtomicBool::new(false));
    let start = Instant::now();

    // Warm a connection per worker up front so the ramp-up is not dominated
    // by handshake latency.
    let mut conns = Vec::new();
    for _ in 0..workers {
        conns.push(pool.get_conn().await?);
    }

    let mut handles = Vec::new();
    for mut conn in conns {
        let table_cursor = Arc::clone(&table_cursor);
        let seq = Arc::clone(&seq);
        let inserted = Arc::clone(&inserted);
        let failed = Arc::clone(&failed);
        let stop = Arc::clone(&stop);
        let databases = databases.clone();
        let deadline = start + duration;
        // Per-worker tick period keeps the aggregate at the target rate.
        let period = Duration::from_secs_f64((workers as f64 * batch as f64) / rate.max(1.0));
        let mut errors_reported = 0u32;

        handles.push(tokio::spawn(async move {
            let mut ticker = tokio::time::interval(period);
            ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            ticker.tick().await; // the first tick completes immediately
            while Instant::now() < deadline && !stop.load(Ordering::Relaxed) {
                ticker.tick().await;
                let n = table_cursor.fetch_add(1, Ordering::Relaxed);
                let db = &databases[(n as usize) % databases.len()];
                let table_idx = (n / databases.len() as u64) % tables;
                let mut sql = format!(
                    "INSERT INTO `{}`.`t{:03}` (ts_ms, seq, payload) VALUES ",
                    db, table_idx
                );
                let mut values = Vec::with_capacity(batch as usize);
                for _ in 0..batch {
                    let ts = now_ms();
                    let s = seq.fetch_add(1, Ordering::Relaxed);
                    values.push(format!("({},{},'{}')", ts, s, payload(s)));
                }
                sql.push_str(&values.join(","));
                match conn.query_drop(sql).await {
                    Ok(()) => {
                        inserted.fetch_add(batch, Ordering::Relaxed);
                    }
                    Err(e) => {
                        failed.fetch_add(batch, Ordering::Relaxed);
                        if errors_reported < 5 {
                            errors_reported += 1;
                            eprintln!("stress-gen: insert failed: {}", e);
                        }
                    }
                }
            }
        }));
    }

    // Live progress line every 5 seconds.
    let progress = {
        let inserted = Arc::clone(&inserted);
        let failed = Arc::clone(&failed);
        let stop = Arc::clone(&stop);
        tokio::spawn(async move {
            let mut last = 0u64;
            while !stop.load(Ordering::Relaxed) {
                tokio::time::sleep(Duration::from_secs(5)).await;
                let now = inserted.load(Ordering::Relaxed);
                eprintln!(
                    "stress-gen: t={:5}s total={} (+{}), failed={}",
                    0,
                    now,
                    now.saturating_sub(last),
                    failed.load(Ordering::Relaxed)
                );
                last = now;
            }
        })
    };

    for handle in handles {
        let _ = handle.await;
    }
    stop.store(true, Ordering::Relaxed);
    let _ = progress.await;

    let elapsed = start.elapsed().as_secs_f64();
    let total = inserted.load(Ordering::Relaxed);
    let failures = failed.load(Ordering::Relaxed);
    let _ = pool.disconnect().await;

    let summary = serde_json::json!({
        "rows": total,
        "failed": failures,
        "elapsed_sec": elapsed,
        "rows_per_sec": total as f64 / elapsed,
        "target_rate": rate,
        "databases": databases.len(),
        "tables": tables,
        "batch": batch,
        "workers": workers,
    });
    println!("{}", serde_json::to_string_pretty(&summary)?);
    if !out_file.is_empty() {
        std::fs::write(&out_file, serde_json::to_string_pretty(&summary)?)?;
    }
    Ok(())
}
