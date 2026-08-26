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

//! Job Coordinator: compiles job config into executable task descriptors,
//! schedules them onto workers, and manages the job lifecycle.
//!
//! Mirrors Java's JobMaster + ExecutionPlanGenerator.
//!
//! ## Execution model
//!
//! Each job compiles to `parallelism` **chained pipeline tasks**: every task
//! runs Source → Transforms → Sink inside one `TaskGroup` on one worker
//! (operator chaining, like Flink/SeaTunnel). This gives real end-to-end
//! dataflow without inter-stage network channels while still scaling out
//! across workers by subtask index.
//!
//! ## Dispatch protocol
//!
//! Workers pull pending tasks via their heartbeat. A task moves through:
//! `Created → Deploying (handed to worker) → Running → Completed|Failed|Cancelled`.
//! Only `Created` tasks are ever handed out, so heartbeats never double-assign.

use std::collections::HashMap;
use std::sync::Arc;

use parking_lot::RwLock;
use tracing::{error, info, warn};

use seatunnel_engine_comm::TaskDescriptor;

/// Default checkpoint interval applied when the job config does not set one.
pub const DEFAULT_CHECKPOINT_INTERVAL_MS: u64 = 30_000;

/// State of a running job / task.
#[derive(Debug, PartialEq, Clone)]
pub enum JobState {
    Created,
    Scheduled,
    Deploying,
    Running,
    Completed,
    Failed { reason: String },
    Cancelled,
}

impl From<&str> for JobState {
    fn from(s: &str) -> Self {
        match s {
            "RUNNING" => JobState::Running,
            "COMPLETED" => JobState::Completed,
            "FAILED" => JobState::Failed { reason: "unknown".to_string() },
            "CANCELLED" => JobState::Cancelled,
            "SCHEDULED" => JobState::Scheduled,
            "DEPLOYING" => JobState::Deploying,
            _ => JobState::Created,
        }
    }
}

impl JobState {
    /// Proto `JobState` enum value.
    pub fn to_proto_state(&self) -> i32 {
        match self {
            JobState::Created => 1,
            JobState::Scheduled => 2,
            JobState::Deploying => 2,
            JobState::Running => 3,
            JobState::Completed => 4,
            JobState::Failed { .. } => 5,
            JobState::Cancelled => 6,
        }
    }

    pub fn to_wire(&self) -> &'static str {
        match self {
            JobState::Created => "CREATED",
            JobState::Scheduled => "SCHEDULED",
            JobState::Deploying => "DEPLOYING",
            JobState::Running => "RUNNING",
            JobState::Completed => "COMPLETED",
            JobState::Failed { .. } => "FAILED",
            JobState::Cancelled => "CANCELLED",
        }
    }

    pub fn is_terminal(&self) -> bool {
        matches!(
            self,
            JobState::Completed | JobState::Failed { .. } | JobState::Cancelled
        )
    }
}

/// Task execution info tracked by the coordinator.
#[derive(Debug, Clone)]
pub struct TaskInfo {
    pub task_id: String,
    pub stage_id: String,
    pub worker_id: String,
    pub state: JobState,
    pub processed_records: u64,
    pub error: Option<String>,
}

/// A running job with its full state.
#[derive(Clone)]
pub struct RunningJob {
    pub job_id: String,
    pub job_name: String,
    pub state: JobState,
    pub parallelism: usize,
    pub tasks: HashMap<String, TaskInfo>,
    pub start_time: i64,
    pub end_time: Option<i64>,
    pub error_message: Option<String>,
    pub checkpoint_interval_ms: u64,
    pub checkpoints_completed: u64,
}

/// Job coordinator managing the lifecycle of all submitted jobs.
pub struct JobCoordinator {
    jobs: RwLock<HashMap<String, RunningJob>>,
    /// Full descriptors by task_id — handed to workers at dispatch time.
    all_tasks: RwLock<HashMap<String, TaskDescriptor>>,
}

impl JobCoordinator {
    pub fn new() -> Self {
        JobCoordinator {
            jobs: RwLock::new(HashMap::new()),
            all_tasks: RwLock::new(HashMap::new()),
        }
    }

    /// Compile a job config into chained task descriptors and register the job.
    ///
    /// Fails when no worker has registered yet — silently queueing would leave
    /// users staring at a SCHEDULED job forever.
    pub fn compile_and_schedule(
        &self,
        job_id: &str,
        job_name: &str,
        config: &serde_json::Value,
        parallelism_override: Option<usize>,
        workers: &[(String, String)],
    ) -> anyhow::Result<(String, Vec<TaskDescriptor>)> {
        let parallelism = parallelism_override
            .unwrap_or_else(|| env_parallelism(config))
            .max(1);
        if workers.is_empty() {
            anyhow::bail!(
                "no worker registered: cannot schedule job '{}' — start a worker with \
                 `seatunnel-engine-server --role worker --master <master>`",
                job_name
            );
        }

        let checkpoint_interval = env_checkpoint_interval(config);
        let source = extract_first_section(config, "source")
            .ok_or_else(|| anyhow::anyhow!("job config has no source section"))?;
        let sink = extract_first_section(config, "sink")
            .ok_or_else(|| anyhow::anyhow!("job config has no sink section"))?;
        let transforms = extract_transform_list(config);

        info!(
            "Compiling job {} '{}': parallelism={} checkpoint={}ms source='{}' sink='{}' transforms={}",
            job_id,
            job_name,
            parallelism,
            checkpoint_interval,
            source.0,
            sink.0,
            transforms.len()
        );

        // Build task descriptors — one per subtask, round-robin over workers.
        let mut tasks = Vec::with_capacity(parallelism);
        for i in 0..parallelism {
            let (worker_id, worker_addr) = &workers[i % workers.len()];
            tasks.push(TaskDescriptor {
                task_id: format!("{}-{}", job_id, i),
                job_id: job_id.to_string(),
                stage_id: format!("{}-pipeline", job_id),
                task_name: format!("{}-pipeline-{}", job_name, i),
                task_index: i as i32,
                source_config_json: String::new(),
                sink_config_json: String::new(),
                parallelism: parallelism as i32,
                config: HashMap::from([
                    ("worker_id".to_string(), worker_id.clone()),
                    ("worker_address".to_string(), worker_addr.clone()),
                    ("job.name".to_string(), job_name.to_string()),
                    ("source.plugin".to_string(), source.0.clone()),
                    (
                        "source.config".to_string(),
                        serde_json::to_string(&source.1)
                            .map_err(|e| anyhow::anyhow!("serialize source config: {}", e))?,
                    ),
                    (
                        "sink.plugin".to_string(),
                        sink.0.clone(),
                    ),
                    (
                        "sink.config".to_string(),
                        serde_json::to_string(&sink.1)
                            .map_err(|e| anyhow::anyhow!("serialize sink config: {}", e))?,
                    ),
                    (
                        "transform.config".to_string(),
                        serde_json::to_string(&transforms)
                            .map_err(|e| anyhow::anyhow!("serialize transforms: {}", e))?,
                    ),
                    (
                        "checkpoint.interval".to_string(),
                        checkpoint_interval.to_string(),
                    ),
                ]),
            });
        }

        // Register the job + task infos.
        let now = seatunnel_engine_core::now_millis();
        let mut task_infos = HashMap::new();
        for t in &tasks {
            task_infos.insert(
                t.task_id.clone(),
                TaskInfo {
                    task_id: t.task_id.clone(),
                    stage_id: t.stage_id.clone(),
                    worker_id: t.config.get("worker_id").cloned().unwrap_or_default(),
                    state: JobState::Scheduled,
                    processed_records: 0,
                    error: None,
                },
            );
        }

        let job = RunningJob {
            job_id: job_id.to_string(),
            job_name: job_name.to_string(),
            state: JobState::Scheduled,
            parallelism,
            tasks: task_infos,
            start_time: now,
            end_time: None,
            error_message: None,
            checkpoint_interval_ms: checkpoint_interval,
            checkpoints_completed: 0,
        };

        self.jobs.write().insert(job_id.to_string(), job);
        {
            let mut all = self.all_tasks.write();
            for t in &tasks {
                all.insert(t.task_id.clone(), t.clone());
            }
        }

        Ok((job_id.to_string(), tasks))
    }

    /// Get a job snapshot.
    pub fn get_job(&self, job_id: &str) -> Option<RunningJob> {
        self.jobs.read().get(job_id).cloned()
    }

    /// List all jobs.
    pub fn list_jobs(&self) -> Vec<RunningJob> {
        self.jobs.read().values().cloned().collect()
    }

    /// IDs of cancelled jobs whose workers still need to be told to stop.
    pub fn cancelled_job_ids(&self) -> Vec<String> {
        self.jobs
            .read()
            .values()
            .filter(|j| j.state == JobState::Cancelled)
            .map(|j| j.job_id.clone())
            .collect()
    }

    /// Pending (never-dispatched) tasks assigned to `worker_id`.
    pub fn get_pending_tasks_for_worker(&self, worker_id: &str) -> Vec<TaskDescriptor> {
        let jobs = self.jobs.read();
        let all = self.all_tasks.read();
        let mut out = Vec::new();
        for job in jobs.values() {
            if job.state.is_terminal() {
                continue;
            }
            for info in job.tasks.values() {
                if info.state != JobState::Scheduled && info.state != JobState::Created {
                    continue;
                }
                if let Some(desc) = all.get(&info.task_id) {
                    if desc.config.get("worker_id").map(|w| w == worker_id).unwrap_or(false) {
                        out.push(desc.clone());
                    }
                }
            }
        }
        out
    }

    /// Mark tasks as handed to a worker so they are never re-dispatched.
    pub fn mark_tasks_dispatched(&self, task_ids: &[String], worker_id: &str) {
        let mut jobs = self.jobs.write();
        for tid in task_ids {
            if let Some(job) = jobs.values_mut().find(|j| j.tasks.contains_key(tid)) {
                if let Some(info) = job.tasks.get_mut(tid) {
                    info.state = JobState::Deploying;
                    info.worker_id = worker_id.to_string();
                }
                if job.state == JobState::Scheduled || job.state == JobState::Created {
                    job.state = JobState::Deploying;
                }
            }
        }
    }

    /// Update task status from a worker report and roll up job state.
    pub fn report_task_status(
        &self,
        job_id: &str,
        task_id: &str,
        state: &str,
        records: u64,
        error: Option<String>,
    ) {
        let mut jobs = self.jobs.write();
        let Some(job) = jobs.get_mut(job_id) else {
            warn!("status report for unknown job {}", job_id);
            return;
        };

        if let Some(task) = job.tasks.get_mut(task_id) {
            task.state = JobState::from(state);
            task.processed_records = records.max(task.processed_records);
            task.error = error;
        } else {
            warn!("status report for unknown task {} in job {}", task_id, job_id);
        }

        let total = job.tasks.len();
        let failed = job
            .tasks
            .values()
            .find(|t| matches!(t.state, JobState::Failed { .. }))
            .cloned();
        let running_or_more = job.tasks.values().any(|t| {
            matches!(t.state, JobState::Running | JobState::Deploying | JobState::Completed)
        });
        let completed = job.tasks.values().filter(|t| t.state == JobState::Completed).count();

        match failed {
            Some(f) => {
                let reason = f.error.clone().unwrap_or_else(|| "task failed".into());
                if !job.state.is_terminal() {
                    error!("Job {} failed: task {}: {}", job_id, f.task_id, reason);
                    job.state = JobState::Failed { reason: reason.clone() };
                    job.end_time = Some(seatunnel_engine_core::now_millis());
                    job.error_message = Some(reason);
                }
            }
            None => {
                if total > 0 && completed == total {
                    if !job.state.is_terminal() {
                        info!("Job {} completed: all {} tasks done", job_id, total);
                        job.state = JobState::Completed;
                        job.end_time = Some(seatunnel_engine_core::now_millis());
                    }
                } else if running_or_more && job.state != JobState::Running {
                    job.state = JobState::Running;
                }
            }
        }
    }

    /// Record a checkpoint reported by a worker.
    pub fn report_checkpoint(&self, job_id: &str, _task_id: &str, checkpoint_id: u64, success: bool) {
        let mut jobs = self.jobs.write();
        if let Some(job) = jobs.get_mut(job_id) {
            if success {
                job.checkpoints_completed += 1;
                tracing::debug!(
                    "Job {} checkpoint {} recorded (total={})",
                    job_id,
                    checkpoint_id,
                    job.checkpoints_completed
                );
            } else {
                tracing::warn!("Job {} checkpoint {} reported as failed", job_id, checkpoint_id);
            }
        }
    }

    /// Cancel a job. Returns true if a running/scheduled job was cancelled.
    pub fn cancel_job(&self, job_id: &str) -> bool {
        let mut jobs = self.jobs.write();
        if let Some(job) = jobs.get_mut(job_id) {
            if !job.state.is_terminal() {
                job.state = JobState::Cancelled;
                job.end_time = Some(seatunnel_engine_core::now_millis());
                info!("Job {} cancelled by user", job_id);
                return true;
            }
        }
        false
    }
}

/// Extract `(plugin_name, config_value)` of the first entry of a section
/// (`source` / `sink`). Accepts both `{Plugin: {...}}` maps and arrays of
/// single-key objects.
fn extract_first_section<'a>(
    config: &'a serde_json::Value,
    section: &str,
) -> Option<(String, &'a serde_json::Value)> {
    let sec = config.get(section)?;
    match sec {
        serde_json::Value::Object(map) => map.iter().next().map(|(k, v)| (k.clone(), v)),
        serde_json::Value::Array(items) => items.first().and_then(|item| {
            item.as_object()
                .and_then(|m| m.iter().next())
                .map(|(k, v)| (k.clone(), v))
        }),
        _ => None,
    }
}

/// Normalize the transform section into an ordered array of configs.
fn extract_transform_list(config: &serde_json::Value) -> Vec<serde_json::Value> {
    let Some(sec) = config.get("transform") else {
        return Vec::new();
    };
    match sec {
        serde_json::Value::Array(items) => items.clone(),
        serde_json::Value::Object(map) => map
            .iter()
            .map(|(name, cfg)| match cfg {
                serde_json::Value::Object(inner) => {
                    let mut full = inner.clone();
                    full.insert("plugin_name".into(), serde_json::Value::String(name.clone()));
                    serde_json::Value::Object(full)
                }
                other => serde_json::json!({ "plugin_name": name, "config": other }),
            })
            .collect(),
        _ => Vec::new(),
    }
}

/// Read `env.parallelism.default` / `env.parallelism` from a job config.
pub fn env_parallelism(config: &serde_json::Value) -> usize {
    config
        .get("env")
        .and_then(|env| {
            env.get("parallelism").and_then(|p| match p {
                serde_json::Value::Number(n) => n.as_u64(),
                serde_json::Value::Object(m) => m.get("default").and_then(|d| d.as_u64()),
                _ => None,
            })
        })
        .map(|v| v as usize)
        .unwrap_or(1)
}

/// Read `env.checkpoint.interval` from a job config.
pub fn env_checkpoint_interval(config: &serde_json::Value) -> u64 {
    config
        .get("env")
        .and_then(|env| {
            env.get("checkpoint").and_then(|c| match c {
                serde_json::Value::Number(n) => n.as_u64(),
                serde_json::Value::Object(m) => m.get("interval").and_then(|i| i.as_u64()),
                _ => None,
            })
        })
        .unwrap_or(DEFAULT_CHECKPOINT_INTERVAL_MS)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn workers(n: usize) -> Vec<(String, String)> {
        (0..n)
            .map(|i| (format!("worker-{}", i), format!("127.0.0.1:{}", 5001 + i)))
            .collect()
    }

    #[test]
    fn test_compile_chained_tasks() {
        let coordinator = JobCoordinator::new();
        let config = json!({
            "env": { "job.name": "cdc-job", "parallelism": 2, "checkpoint": { "interval": 10000 } },
            "source": { "MySQL-CDC": { "hostname": "db", "port": 3306, "database-name": "s", "table-name": "t" } },
            "sink": { "Kafka": { "bootstrap.servers": "k:9092", "topic": "out" } }
        });

        let (job_id, tasks) = coordinator
            .compile_and_schedule("j1", "cdc-job", &config, None, &workers(3))
            .unwrap();
        assert_eq!(job_id, "j1");
        assert_eq!(tasks.len(), 2);

        let t0 = &tasks[0];
        assert_eq!(t0.config.get("source.plugin").unwrap(), "MySQL-CDC");
        assert_eq!(t0.config.get("sink.plugin").unwrap(), "Kafka");
        assert_eq!(t0.config.get("checkpoint.interval").unwrap(), "10000");
        let src: serde_json::Value =
            serde_json::from_str(t0.config.get("source.config").unwrap()).unwrap();
        assert_eq!(src["hostname"], "db");

        // Round-robin placement across three workers.
        assert_eq!(tasks[0].config.get("worker_id").unwrap(), "worker-0");
        assert_eq!(tasks[1].config.get("worker_id").unwrap(), "worker-1");

        // Dispatch dedup: pending once, gone after marking dispatched.
        let pending = coordinator.get_pending_tasks_for_worker("worker-0");
        assert_eq!(pending.len(), 1);
        coordinator.mark_tasks_dispatched(
            &pending.iter().map(|t| t.task_id.clone()).collect::<Vec<_>>(),
            "worker-0",
        );
        assert!(coordinator.get_pending_tasks_for_worker("worker-0").is_empty());
    }

    #[test]
    fn test_no_workers_is_error() {
        let coordinator = JobCoordinator::new();
        let config = json!({
            "source": { "Fake": { "row.num": 10 } },
            "sink": { "Console": {} }
        });
        let result = coordinator.compile_and_schedule("jx", "x", &config, None, &[]);
        assert!(result.is_err());
    }

    #[test]
    fn test_status_rollup_running_and_completed() {
        let coordinator = JobCoordinator::new();
        let config = json!({
            "source": { "Fake": {} },
            "sink": { "Console": {} }
        });
        let (job_id, tasks) = coordinator
            .compile_and_schedule("jr", "r", &config, Some(2), &workers(1))
            .unwrap();
        for t in &tasks {
            coordinator.mark_tasks_dispatched(&[t.task_id.clone()], "worker-0");
        }
        let job = coordinator.get_job(&job_id).unwrap();
        assert_eq!(job.state, JobState::Deploying);

        for t in &tasks {
            coordinator.report_task_status(&job_id, &t.task_id, "RUNNING", 5, None);
        }
        assert_eq!(coordinator.get_job(&job_id).unwrap().state, JobState::Running);

        for t in &tasks {
            coordinator.report_task_status(&job_id, &t.task_id, "COMPLETED", 50, None);
        }
        assert_eq!(coordinator.get_job(&job_id).unwrap().state, JobState::Completed);
    }

    #[test]
    fn test_failed_task_fails_job() {
        let coordinator = JobCoordinator::new();
        let config = json!({
            "source": { "Fake": {} },
            "sink": { "Console": {} }
        });
        let (job_id, tasks) = coordinator
            .compile_and_schedule("jf", "f", &config, Some(1), &workers(1))
            .unwrap();
        coordinator.mark_tasks_dispatched(&[tasks[0].task_id.clone()], "worker-0");
        coordinator.report_task_status(
            &job_id,
            &tasks[0].task_id,
            "FAILED",
            0,
            Some("connection refused".into()),
        );
        let job = coordinator.get_job(&job_id).unwrap();
        assert!(matches!(job.state, JobState::Failed { .. }));
        assert_eq!(job.error_message.as_deref(), Some("connection refused"));
    }

    #[test]
    fn test_cancel_propagates_to_worker_notification() {
        let coordinator = JobCoordinator::new();
        let config = json!({
            "source": { "Fake": {} },
            "sink": { "Console": {} }
        });
        let (job_id, _) = coordinator
            .compile_and_schedule("jc", "c", &config, Some(1), &workers(1))
            .unwrap();
        assert!(coordinator.cancel_job(&job_id));
        assert_eq!(coordinator.cancelled_job_ids(), vec![job_id]);
    }

    #[test]
    fn test_env_parsing_shapes() {
        assert_eq!(env_parallelism(&json!({"env":{"parallelism":4}})), 4);
        assert_eq!(env_parallelism(&json!({"env":{"parallelism":{"default":3}}})), 3);
        assert_eq!(env_parallelism(&json!({})), 1);
        assert_eq!(env_checkpoint_interval(&json!({"env":{"checkpoint":{"interval":5000}}})), 5000);
        assert_eq!(env_checkpoint_interval(&json!({"env":{"checkpoint":12345}})), 12345);
        assert_eq!(
            env_checkpoint_interval(&json!({})),
            DEFAULT_CHECKPOINT_INTERVAL_MS
        );
    }
}
