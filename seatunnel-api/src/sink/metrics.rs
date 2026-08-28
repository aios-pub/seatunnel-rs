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

use std::collections::VecDeque;
use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

/// Sliding-window length for the counting metrics (seconds).
pub const METRICS_WINDOW_SECS: u64 = 60;

/// EMA smoothing factor for delivery latency (weights recent samples).
const LATENCY_EMA_ALPHA: f64 = 0.2;

/// A point-in-time view of [`SinkMetrics`] (windowed, not lifetime).
#[derive(Debug, Clone, Default, PartialEq)]
pub struct SinkMetricsSnapshot {
    /// The sliding window length the counts below cover.
    pub window_secs: u64,
    /// Messages enqueued to the producer within the window.
    pub sent: u64,
    /// Delivery reports acknowledged OK within the window.
    pub delivered: u64,
    /// Delivery reports failed within the window.
    pub failed: u64,
    /// Messages currently in flight (enqueued, report pending).
    pub in_flight: u64,
    /// Exponential moving average of send→report latency, millis.
    pub latency_ema_ms: f64,
    /// Maximum send→report latency within the window, millis.
    pub latency_max_ms: u64,
    /// Detailed last delivery error (full message + context).
    pub last_error: Option<String>,
    /// Epoch millis of `last_error`.
    pub last_error_at: i64,
}

/// Per-second bucket of a windowed counter.
#[derive(Debug, Default)]
struct Bucket {
    epoch_sec: u64,
    count: u64,
    max_ms: u64,
}

impl Bucket {
    fn is_expired(&self, now_sec: u64) -> bool {
        now_sec.saturating_sub(self.epoch_sec) >= METRICS_WINDOW_SECS
    }
}

/// Windowed counter: per-second buckets, aggregated into one entry per
/// active second; expired buckets are pruned on write and on snapshot.
#[derive(Debug, Default)]
struct WindowedCounter {
    buckets: VecDeque<Bucket>,
}

impl WindowedCounter {
    fn record(&mut self, epoch_sec: u64, count: u64, max_ms: u64) {
        self.prune(epoch_sec);
        match self.buckets.back_mut() {
            Some(bucket) if bucket.epoch_sec == epoch_sec => {
                bucket.count += count;
                bucket.max_ms = bucket.max_ms.max(max_ms);
            }
            _ => self.buckets.push_back(Bucket {
                epoch_sec,
                count,
                max_ms,
            }),
        }
    }

    fn prune(&mut self, now_sec: u64) {
        while self.buckets.front().is_some_and(|b| b.is_expired(now_sec)) {
            self.buckets.pop_front();
        }
    }

    /// Sum of counts within the window (prunes first).
    fn sum(&mut self, now_sec: u64) -> u64 {
        self.prune(now_sec);
        self.buckets.iter().map(|b| b.count).sum()
    }

    /// Max of bucket maxima within the window (prunes first).
    fn max(&mut self, now_sec: u64) -> u64 {
        self.prune(now_sec);
        self.buckets.iter().map(|b| b.max_ms).max().unwrap_or(0)
    }
}

/// Shared, connector-agnostic sink metrics handle.
///
/// Writers record events in batches (one lock acquisition per flush, not
/// per message); the task layer snapshots every status publish (~200ms).
/// Counters are SLIDING-WINDOW (last
/// [`METRICS_WINDOW_SECS`] seconds), not lifetime totals — lifetime
/// totals belong to checkpoint state.
///
/// The handle is a plain shared struct (no registry/global state): the
/// task creates one [`Arc<SinkMetrics>`], hands it to the writer via
/// [`crate::sink::SinkWriterContext`] and snapshots it into task status.
#[derive(Debug, Default)]
pub struct SinkMetrics {
    inner: Mutex<MetricsInner>,
    in_flight: AtomicU64,
}

#[derive(Debug, Default)]
struct MetricsInner {
    sent: WindowedCounter,
    delivered: WindowedCounter,
    failed: WindowedCounter,
    latency_ema_ms: f64,
    last_error: Option<String>,
    last_error_at: i64,
}

impl SinkMetrics {
    pub fn new() -> Self {
        Self::default()
    }

    fn now_sec() -> u64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or(0)
    }

    fn now_ms() -> i64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_millis() as i64)
            .unwrap_or(0)
    }

    /// Record `count` messages enqueued to the producer (raises the
    /// in-flight gauge by the same amount).
    pub fn record_sent(&self, count: u64) {
        if count == 0 {
            return;
        }
        let now = Self::now_sec();
        if let Ok(mut inner) = self.inner.lock() {
            inner.sent.record(now, count, 0);
        }
        self.adjust_in_flight(count as i64);
    }

    /// Record delivery outcomes: acknowledged OK (with per-delivery
    /// latencies, millis) and failures (with the detailed last error).
    /// Each report lowers the in-flight gauge.
    pub fn record_deliveries(
        &self,
        delivered: u64,
        failed: u64,
        latencies_ms: &[u64],
        last_error: Option<&str>,
    ) {
        let now = Self::now_sec();
        let max_latency = latencies_ms.iter().copied().max().unwrap_or(0);
        if let Ok(mut inner) = self.inner.lock() {
            if delivered > 0 {
                inner.delivered.record(now, delivered, 0);
            }
            if failed > 0 {
                inner.failed.record(now, failed, 0);
            }
            if max_latency > 0 {
                // Latency max rides the delivered-counter buckets so the
                // window applies to it too.
                inner.delivered.record(now, 0, max_latency);
            }
            for &latency in latencies_ms {
                if inner.latency_ema_ms == 0.0 {
                    inner.latency_ema_ms = latency as f64;
                } else {
                    inner.latency_ema_ms +=
                        LATENCY_EMA_ALPHA * (latency as f64 - inner.latency_ema_ms);
                }
            }
            if let Some(error) = last_error {
                inner.last_error = Some(error.to_string());
                inner.last_error_at = Self::now_ms();
            }
        }
        self.adjust_in_flight(-((delivered + failed) as i64));
    }

    /// Adjust the in-flight gauge (enqueued minus reports received).
    fn adjust_in_flight(&self, delta: i64) {
        if delta >= 0 {
            self.in_flight.fetch_add(delta as u64, Ordering::Relaxed);
        } else {
            let sub = (-delta) as u64;
            // saturating_sub semantics via CAS loop
            let mut current = self.in_flight.load(Ordering::Relaxed);
            loop {
                let next = current.saturating_sub(sub);
                match self.in_flight.compare_exchange_weak(
                    current,
                    next,
                    Ordering::Relaxed,
                    Ordering::Relaxed,
                ) {
                    Ok(_) => return,
                    Err(actual) => current = actual,
                }
            }
        }
    }

    /// Point-in-time windowed snapshot.
    pub fn snapshot(&self) -> SinkMetricsSnapshot {
        let now = Self::now_sec();
        let in_flight = self.in_flight.load(Ordering::Relaxed);
        match self.inner.lock() {
            Ok(mut inner) => SinkMetricsSnapshot {
                window_secs: METRICS_WINDOW_SECS,
                sent: inner.sent.sum(now),
                delivered: inner.delivered.sum(now),
                failed: inner.failed.sum(now),
                in_flight,
                latency_ema_ms: inner.latency_ema_ms,
                latency_max_ms: inner.delivered.max(now),
                last_error: inner.last_error.clone(),
                last_error_at: inner.last_error_at,
            },
            Err(_) => SinkMetricsSnapshot {
                window_secs: METRICS_WINDOW_SECS,
                in_flight,
                ..Default::default()
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn windowed_counter_sums_and_prunes() {
        let mut counter = WindowedCounter::default();
        counter.record(100, 5, 0);
        counter.record(100, 2, 0);
        counter.record(101, 1, 0);
        assert_eq!(counter.sum(101), 8);
        // 61s later everything expired.
        assert_eq!(counter.sum(161), 0);
        counter.record(161, 3, 0);
        assert_eq!(counter.sum(161), 3);
    }

    #[test]
    fn snapshot_counts_window_and_latency_max() {
        let metrics = SinkMetrics::new();
        metrics.record_sent(10);
        metrics.record_deliveries(8, 2, &[3, 7, 12], Some("boom topic=t key=k"));
        let snapshot = metrics.snapshot();
        assert_eq!(snapshot.sent, 10);
        assert_eq!(snapshot.delivered, 8);
        assert_eq!(snapshot.failed, 2);
        assert_eq!(snapshot.in_flight, 0, "all reports arrived");
        assert_eq!(snapshot.latency_max_ms, 12);
        assert!(snapshot.latency_ema_ms > 0.0 && snapshot.latency_ema_ms <= 12.0);
        assert_eq!(snapshot.last_error.as_deref(), Some("boom topic=t key=k"));
        assert!(snapshot.last_error_at > 0);
    }

    #[test]
    fn in_flight_never_goes_negative() {
        let metrics = SinkMetrics::new();
        // Failure reports without matching sends must not underflow.
        metrics.record_deliveries(0, 5, &[], None);
        assert_eq!(metrics.snapshot().in_flight, 0);
        metrics.record_sent(3);
        metrics.record_deliveries(0, 10, &[], None);
        assert_eq!(metrics.snapshot().in_flight, 0);
    }
}
