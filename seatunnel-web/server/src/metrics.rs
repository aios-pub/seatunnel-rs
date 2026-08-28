/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.
 */

//! Prometheus metrics: HTTP server metrics plus engine gauges refreshed
//! by a background poller.

use std::collections::{HashMap, HashSet};
use std::time::Duration;

use prometheus::{
    Encoder, GaugeVec, HistogramOpts, HistogramVec, IntGauge, IntGaugeVec, Opts, Registry,
    TextEncoder,
};

use crate::AppState;
use crate::engine::EngineOps;

/// All metrics exposed by the web console. Clone shares the same registry.
#[derive(Clone)]
pub struct Metrics {
    registry: Registry,
    http_requests_total: prometheus::IntCounterVec,
    http_request_duration_seconds: HistogramVec,
    jobs: IntGaugeVec,
    workers: IntGauge,
    running_tasks: IntGauge,
    /// Dynamic admission gauges per worker (measured pressure, not slots).
    worker_load_score: IntGaugeVec,
    worker_overloaded: IntGaugeVec,
    worker_lag_ms: IntGaugeVec,
    worker_mem_ratio: IntGaugeVec,
    /// Worker labels seen last refresh (to drop disappeared workers).
    worker_labels: std::sync::Arc<std::sync::Mutex<HashSet<String>>>,
    task_processed_records: IntGaugeVec,
    task_records_per_second: IntGaugeVec,
    task_idle_seconds: IntGaugeVec,
    /// Sink delivery metrics per task (60s window; only set when the
    /// sink reports — e.g. the Kafka connector's in-flight pipeline).
    task_sink_sent: IntGaugeVec,
    task_sink_delivered: IntGaugeVec,
    task_sink_failed: IntGaugeVec,
    task_sink_in_flight: IntGaugeVec,
    task_sink_delivery_latency_ema_ms: GaugeVec,
    task_sink_delivery_latency_max_ms: IntGaugeVec,
    /// Label sets present in `task_processed_records` after the last refresh.
    task_labels: std::sync::Arc<std::sync::Mutex<HashSet<(String, String)>>>,
    /// Previous `(processed_records, sampled_at)` per task for rate gauges.
    rate_samples:
        std::sync::Arc<std::sync::Mutex<HashMap<(String, String), (i64, std::time::Instant)>>>,
    /// Poller liveness: cycles run, cycles that failed to reach the
    /// master, and when the last successful refresh completed. A stale
    /// `/metrics` page is diagnosable from the metrics themselves.
    refresh_cycles: prometheus::IntCounter,
    refresh_failures: prometheus::IntCounter,
    refresh_last_ok_unix_ts: IntGauge,
}

impl Metrics {
    /// Create and register all metric families.
    pub fn new() -> Self {
        let registry = Registry::new();

        let http_requests_total = prometheus::IntCounterVec::new(
            Opts::new(
                "seatunnel_web_http_requests_total",
                "HTTP requests handled by the web console",
            ),
            &["method", "code"],
        )
        .unwrap();
        let http_request_duration_seconds = HistogramVec::new(
            HistogramOpts::new(
                "seatunnel_web_http_request_duration_seconds",
                "HTTP request latency in seconds",
            ),
            &["method"],
        )
        .unwrap();
        let jobs = IntGaugeVec::new(
            Opts::new("seatunnel_jobs", "Jobs managed by the cluster"),
            &["state"],
        )
        .unwrap();
        let workers = IntGauge::new("seatunnel_workers", "Registered workers").unwrap();
        let worker_load_score = IntGaugeVec::new(
            Opts::new(
                "seatunnel_worker_load_score",
                "Measured admission pressure per worker (per-mille 0-1000)",
            ),
            &["worker"],
        )
        .unwrap();
        let worker_overloaded = IntGaugeVec::new(
            Opts::new(
                "seatunnel_worker_overloaded",
                "1 while the worker is over an admission watermark (no new tasks)",
            ),
            &["worker"],
        )
        .unwrap();
        let worker_lag_ms = IntGaugeVec::new(
            Opts::new(
                "seatunnel_worker_lag_ms",
                "Event-loop lag EMA in ms (runtime saturation signal)",
            ),
            &["worker"],
        )
        .unwrap();
        let worker_mem_ratio = IntGaugeVec::new(
            Opts::new(
                "seatunnel_worker_mem_ratio",
                "Process RSS over usable memory (per-mille 0-1000)",
            ),
            &["worker"],
        )
        .unwrap();
        let running_tasks = IntGauge::new(
            "seatunnel_running_tasks",
            "Tasks currently running in the cluster",
        )
        .unwrap();
        let task_processed_records = IntGaugeVec::new(
            Opts::new(
                "seatunnel_task_processed_records",
                "Records processed by a task",
            ),
            &["job", "task"],
        )
        .unwrap();
        let task_records_per_second = IntGaugeVec::new(
            Opts::new(
                "seatunnel_task_records_per_second",
                "Task throughput derived from consecutive refreshes",
            ),
            &["job", "task"],
        )
        .unwrap();
        let task_idle_seconds = IntGaugeVec::new(
            Opts::new(
                "seatunnel_task_idle_seconds",
                "Seconds since the task last processed a record (-1 = none yet)",
            ),
            &["job", "task"],
        )
        .unwrap();
        let task_sink_sent = IntGaugeVec::new(
            Opts::new(
                "seatunnel_task_sink_sent",
                "Sink messages enqueued in the last window seconds",
            ),
            &["job", "task"],
        )
        .unwrap();
        let task_sink_delivered = IntGaugeVec::new(
            Opts::new(
                "seatunnel_task_sink_delivered",
                "Sink delivery reports acknowledged OK in the last window seconds",
            ),
            &["job", "task"],
        )
        .unwrap();
        let task_sink_failed = IntGaugeVec::new(
            Opts::new(
                "seatunnel_task_sink_failed",
                "Sink delivery reports failed in the last window seconds",
            ),
            &["job", "task"],
        )
        .unwrap();
        let task_sink_in_flight = IntGaugeVec::new(
            Opts::new(
                "seatunnel_task_sink_in_flight",
                "Sink messages currently in flight (enqueued, report pending)",
            ),
            &["job", "task"],
        )
        .unwrap();
        let task_sink_delivery_latency_ema_ms = GaugeVec::new(
            Opts::new(
                "seatunnel_task_sink_delivery_latency_ema_ms",
                "Sink EMA of enqueue-to-report latency, millis",
            ),
            &["job", "task"],
        )
        .unwrap();
        let task_sink_delivery_latency_max_ms = IntGaugeVec::new(
            Opts::new(
                "seatunnel_task_sink_delivery_latency_max_ms",
                "Sink max enqueue-to-report latency within the window, millis",
            ),
            &["job", "task"],
        )
        .unwrap();
        let refresh_cycles = prometheus::IntCounter::new(
            "seatunnel_web_refresh_cycles_total",
            "Engine refresh cycles attempted by the metrics poller",
        )
        .unwrap();
        let refresh_failures = prometheus::IntCounter::new(
            "seatunnel_web_refresh_failures_total",
            "Engine refresh cycles that failed to list jobs from the master",
        )
        .unwrap();
        let refresh_last_ok_unix_ts = IntGauge::new(
            "seatunnel_web_refresh_last_ok_unix_ts",
            "Unix seconds of the last successful engine refresh (stale gauges are diagnosable)",
        )
        .unwrap();

        registry
            .register(Box::new(http_requests_total.clone()))
            .unwrap();
        registry
            .register(Box::new(http_request_duration_seconds.clone()))
            .unwrap();
        registry.register(Box::new(jobs.clone())).unwrap();
        registry.register(Box::new(workers.clone())).unwrap();
        registry
            .register(Box::new(worker_load_score.clone()))
            .unwrap();
        registry
            .register(Box::new(worker_overloaded.clone()))
            .unwrap();
        registry.register(Box::new(worker_lag_ms.clone())).unwrap();
        registry
            .register(Box::new(worker_mem_ratio.clone()))
            .unwrap();
        registry.register(Box::new(running_tasks.clone())).unwrap();
        registry
            .register(Box::new(task_processed_records.clone()))
            .unwrap();
        registry
            .register(Box::new(task_records_per_second.clone()))
            .unwrap();
        registry
            .register(Box::new(task_idle_seconds.clone()))
            .unwrap();
        registry.register(Box::new(task_sink_sent.clone())).unwrap();
        registry
            .register(Box::new(task_sink_delivered.clone()))
            .unwrap();
        registry
            .register(Box::new(task_sink_failed.clone()))
            .unwrap();
        registry
            .register(Box::new(task_sink_in_flight.clone()))
            .unwrap();
        registry
            .register(Box::new(task_sink_delivery_latency_ema_ms.clone()))
            .unwrap();
        registry
            .register(Box::new(task_sink_delivery_latency_max_ms.clone()))
            .unwrap();
        registry.register(Box::new(refresh_cycles.clone())).unwrap();
        registry
            .register(Box::new(refresh_failures.clone()))
            .unwrap();
        registry
            .register(Box::new(refresh_last_ok_unix_ts.clone()))
            .unwrap();

        Metrics {
            registry,
            http_requests_total,
            http_request_duration_seconds,
            jobs,
            workers,
            worker_load_score,
            worker_overloaded,
            worker_lag_ms,
            worker_mem_ratio,
            worker_labels: std::sync::Arc::new(std::sync::Mutex::new(HashSet::new())),
            running_tasks,
            task_processed_records,
            task_records_per_second,
            task_idle_seconds,
            task_sink_sent,
            task_sink_delivered,
            task_sink_failed,
            task_sink_in_flight,
            task_sink_delivery_latency_ema_ms,
            task_sink_delivery_latency_max_ms,
            task_labels: std::sync::Arc::new(std::sync::Mutex::new(HashSet::new())),
            rate_samples: std::sync::Arc::new(std::sync::Mutex::new(HashMap::new())),
            refresh_cycles,
            refresh_failures,
            refresh_last_ok_unix_ts,
        }
    }

    /// Record one handled HTTP request.
    pub fn record_http(&self, method: &str, code: u16, duration_secs: f64) {
        self.http_requests_total
            .with_label_values(&[method, &code.to_string()])
            .inc();
        self.http_request_duration_seconds
            .with_label_values(&[method])
            .observe(duration_secs);
    }

    /// Encode the registry in the Prometheus text exposition format.
    pub fn gather(&self) -> Option<String> {
        let encoder = TextEncoder::new();
        let mut buffer = Vec::new();
        encoder.encode(&self.registry.gather(), &mut buffer).ok()?;
        String::from_utf8(buffer).ok()
    }

    /// Pull engine state into the gauges once.
    pub async fn refresh(&self, engine: &dyn EngineOps) {
        self.refresh_cycles.inc();
        for state in [
            "CREATED",
            "SCHEDULED",
            "RUNNING",
            "COMPLETED",
            "FAILED",
            "CANCELLED",
        ] {
            self.jobs.with_label_values(&[state]).set(0);
        }

        if let Ok(jobs) = engine.list_jobs().await {
            self.refresh_last_ok_unix_ts.set(
                std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .map(|d| d.as_secs() as i64)
                    .unwrap_or(0),
            );
            let now = std::time::Instant::now();
            let mut live_labels = HashSet::new();
            for job in jobs {
                self.jobs.with_label_values(&[&job.state]).inc();
                if let Ok(status) = engine.job_status(&job.job_id).await {
                    let mut samples = self.rate_samples.lock().unwrap();
                    for task in status.tasks {
                        let key = (job.job_id.clone(), task.task_id.clone());
                        let labels = [job.job_id.as_str(), task.task_id.as_str()];
                        self.task_processed_records
                            .with_label_values(&labels)
                            .set(task.processed_records);
                        // Throughput from consecutive refresh samples.
                        let rate = match samples.get(&key) {
                            Some((prev_records, prev_at))
                                if task.processed_records >= *prev_records =>
                            {
                                let dt = now.duration_since(*prev_at).as_secs_f64();
                                if dt > 0.0 {
                                    ((task.processed_records - prev_records) as f64 / dt) as i64
                                } else {
                                    0
                                }
                            }
                            _ => 0,
                        };
                        self.task_records_per_second
                            .with_label_values(&labels)
                            .set(rate);
                        samples.insert(key, (task.processed_records, now));
                        // Liveness: seconds since the last record.
                        let now_epoch = std::time::SystemTime::now()
                            .duration_since(std::time::UNIX_EPOCH)
                            .map(|d| d.as_millis() as i64)
                            .unwrap_or(0);
                        let idle = if task.last_record_ms > 0 {
                            (now_epoch - task.last_record_ms).max(0) / 1000
                        } else {
                            -1
                        };
                        self.task_idle_seconds.with_label_values(&labels).set(idle);
                        if let Some(m) = &task.sink_metrics {
                            self.task_sink_sent
                                .with_label_values(&labels)
                                .set(m.sent as i64);
                            self.task_sink_delivered
                                .with_label_values(&labels)
                                .set(m.delivered as i64);
                            self.task_sink_failed
                                .with_label_values(&labels)
                                .set(m.failed as i64);
                            self.task_sink_in_flight
                                .with_label_values(&labels)
                                .set(m.in_flight as i64);
                            self.task_sink_delivery_latency_ema_ms
                                .with_label_values(&labels)
                                .set(m.latency_ema_ms);
                            self.task_sink_delivery_latency_max_ms
                                .with_label_values(&labels)
                                .set(m.latency_max_ms as i64);
                        }
                        live_labels.insert((job.job_id.clone(), task.task_id.clone()));
                    }
                }
            }
            // Drop gauges of tasks that disappeared (job finished/evicted).
            let mut previous = self.task_labels.lock().unwrap();
            for (job, task) in previous.difference(&live_labels) {
                let _ = self
                    .task_processed_records
                    .remove_label_values(&[job, task]);
            }
            *previous = live_labels;
        }

        if let Ok(cluster) = engine.cluster_info().await {
            self.workers.set(cluster.available_workers as i64);
            self.running_tasks.set(cluster.running_tasks as i64);
            let mut live: HashSet<String> = HashSet::new();
            for w in &cluster.workers {
                let id = w.worker_id.clone();
                self.worker_load_score
                    .with_label_values(&[&id])
                    .set(w.load_score_permille as i64);
                self.worker_overloaded
                    .with_label_values(&[&id])
                    .set(if w.can_accept { 0 } else { 1 });
                self.worker_lag_ms
                    .with_label_values(&[&id])
                    .set(w.lag_ms as i64);
                self.worker_mem_ratio
                    .with_label_values(&[&id])
                    .set(w.mem_permille as i64);
                live.insert(id);
            }
            // Drop gauges of workers that disappeared.
            let mut previous = self.worker_labels.lock().unwrap();
            for id in previous.difference(&live) {
                let _ = self.worker_load_score.remove_label_values(&[id]);
                let _ = self.worker_overloaded.remove_label_values(&[id]);
                let _ = self.worker_lag_ms.remove_label_values(&[id]);
                let _ = self.worker_mem_ratio.remove_label_values(&[id]);
            }
            *previous = live;
        } else {
            // Unreachable master: gauges keep their last values — count it
            // so a stale console page is visible from the metrics themselves.
            self.refresh_failures.inc();
        }
    }
}

/// Periodically refresh engine gauges until the process exits.
pub fn spawn_poller(state: AppState, interval: Duration) {
    tokio::spawn(async move {
        let mut ticker = tokio::time::interval(interval);
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            ticker.tick().await;
            // A wedged master connection must never freeze the console's
            // metrics (a hung gRPC call without a deadline would park this
            // poller forever and /metrics would serve stale gauges). Bound
            // every refresh cycle by the interval; the next tick retries.
            let _ = tokio::time::timeout(interval, state.metrics.refresh(&*state.engine)).await;
        }
    });
}
