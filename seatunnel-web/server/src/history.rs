/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.
 */

//! In-memory time-series ring for the console's charts.
//!
//! The metrics poller appends one sample per refresh cycle (per task and
//! per worker); the REST endpoints expose the retained window. The data is
//! web-process-local: it resets when the console restarts.

use serde::Serialize;
use std::collections::{HashMap, VecDeque};

/// Default retained samples per series (240 × 5s poll ≈ 20 minutes).
pub const DEFAULT_CAPACITY: usize = 240;

/// One task's sample within a job history point.
#[derive(Debug, Clone, Serialize)]
pub struct TaskPoint {
    pub task_id: String,
    /// Records per second derived between poller samples.
    pub records_per_sec: f64,
    /// Sink delivery latency EMA (ms; 0 when the sink reports nothing).
    pub latency_ema_ms: f64,
    /// Sink delivery latency max within the window (ms).
    pub latency_max_ms: u64,
}

/// One job history sample: every live task's rates at `ts_ms`.
#[derive(Debug, Clone, Serialize)]
pub struct JobPoint {
    pub ts_ms: i64,
    pub tasks: Vec<TaskPoint>,
}

/// Per-job bounded series of points (newest last).
#[derive(Debug, Default)]
struct JobSeries {
    points: VecDeque<JobPoint>,
}

/// One worker's sample within a cluster history point.
#[derive(Debug, Clone, Serialize)]
pub struct WorkerPoint {
    pub worker_id: String,
    pub load_permille: u32,
    pub lag_ms: u32,
    pub mem_permille: u32,
    pub cpu_permille: u32,
}

/// One cluster history sample.
#[derive(Debug, Clone, Serialize)]
pub struct ClusterPoint {
    pub ts_ms: i64,
    pub running_tasks: i32,
    pub workers: Vec<WorkerPoint>,
}

/// The shared ring: per-job series plus the cluster-wide series, each
/// bounded to `capacity` points.
#[derive(Debug)]
pub struct History {
    capacity: usize,
    jobs: std::sync::Mutex<HashMap<String, JobSeries>>,
    cluster: std::sync::Mutex<VecDeque<ClusterPoint>>,
}

/// REST payload for `GET /api/v1/jobs/{id}/history`.
#[derive(Debug, Serialize)]
pub struct JobHistoryDto {
    pub job_id: String,
    pub points: Vec<JobPoint>,
}

/// REST payload for `GET /api/v1/cluster/history`.
#[derive(Debug, Serialize)]
pub struct ClusterHistoryDto {
    pub points: Vec<ClusterPoint>,
}

impl History {
    pub fn new(capacity: usize) -> Self {
        History {
            capacity: capacity.max(2),
            jobs: std::sync::Mutex::new(HashMap::new()),
            cluster: std::sync::Mutex::new(VecDeque::new()),
        }
    }

    /// Record one poller cycle's per-task rates for a job.
    pub fn record_job(
        &self,
        job_id: &str,
        samples: impl IntoIterator<Item = TaskPoint>,
    ) {
        let point = JobPoint {
            ts_ms: now_ms(),
            tasks: samples.into_iter().collect(),
        };
        let mut jobs = self.jobs.lock().unwrap();
        let series = jobs.entry(job_id.to_string()).or_default();
        series.points.push_back(point);
        while series.points.len() > self.capacity {
            series.points.pop_front();
        }
    }

    /// Record one poller cycle's cluster/worker signals.
    pub fn record_cluster(&self, running_tasks: i32, workers: impl IntoIterator<Item = WorkerPoint>) {
        let point = ClusterPoint {
            ts_ms: now_ms(),
            running_tasks,
            workers: workers.into_iter().collect(),
        };
        let mut cluster = self.cluster.lock().unwrap();
        cluster.push_back(point);
        while cluster.len() > self.capacity {
            cluster.pop_front();
        }
    }

    /// Snapshot of one job's retained window (empty when unknown — jobs
    /// leave the ring only when the ring trims or the console restarts).
    pub fn job_snapshot(&self, job_id: &str) -> JobHistoryDto {
        let jobs = self.jobs.lock().unwrap();
        JobHistoryDto {
            job_id: job_id.to_string(),
            points: jobs
                .get(job_id)
                .map(|s| s.points.iter().cloned().collect())
                .unwrap_or_default(),
        }
    }

    /// Snapshot of the retained cluster window.
    pub fn cluster_snapshot(&self) -> ClusterHistoryDto {
        let cluster = self.cluster.lock().unwrap();
        ClusterHistoryDto {
            points: cluster.iter().cloned().collect(),
        }
    }

    /// Drop per-task series of tasks that vanished (job finished and was
    /// deleted); keeps memory bounded when many short jobs come and go.
    pub fn retain_jobs(&self, live_job_ids: &std::collections::HashSet<String>) {
        let mut jobs = self.jobs.lock().unwrap();
        jobs.retain(|job_id, _| live_job_ids.contains(job_id));
    }
}

fn now_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ring_is_bounded_and_ordered() {
        let history = History::new(3);
        for i in 0..5 {
            history.record_job(
                "j",
                [TaskPoint {
                    task_id: "t".to_string(),
                    records_per_sec: i as f64,
                    latency_ema_ms: 0.0,
                    latency_max_ms: 0,
                }],
            );
        }
        let snapshot = history.job_snapshot("j");
        assert_eq!(snapshot.points.len(), 3, "capacity bound");
        assert_eq!(snapshot.points[0].tasks[0].records_per_sec, 2.0);
        assert_eq!(snapshot.points[2].tasks[0].records_per_sec, 4.0);
    }

    #[test]
    fn cluster_ring_snapshot_and_trim() {
        let history = History::new(2);
        history.record_cluster(
            1,
            [WorkerPoint {
                worker_id: "w".to_string(),
                load_permille: 100,
                lag_ms: 5,
                mem_permille: 200,
                cpu_permille: 300,
            }],
        );
        history.record_cluster(2, []);
        history.record_cluster(3, []);
        let snapshot = history.cluster_snapshot();
        assert_eq!(snapshot.points.len(), 2);
        assert_eq!(snapshot.points[0].running_tasks, 2);
        assert_eq!(snapshot.points[1].running_tasks, 3);
    }

    #[test]
    fn retain_drops_vanished_jobs() {
        let history = History::new(4);
        history.record_job("keep", []);
        history.record_job("gone", []);
        let mut live = std::collections::HashSet::new();
        live.insert("keep".to_string());
        history.retain_jobs(&live);
        assert_eq!(history.job_snapshot("keep").points.len(), 1);
        assert_eq!(history.job_snapshot("gone").points.len(), 0);
    }
}
