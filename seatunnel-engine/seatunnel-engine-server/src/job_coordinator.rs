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
            "FAILED" => JobState::Failed {
                reason: "unknown".to_string(),
            },
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
    /// Master-backed shared checkpoint store (storage type = master).
    checkpoint_store: crate::checkpoint_store::MasterCheckpointStore,
}

impl Default for JobCoordinator {
    fn default() -> Self {
        Self::new()
    }
}

pub(crate) struct PipelineSpec {
    name: String,
    parallelism: usize,
    source_plugin: String,
    source_config: serde_json::Value,
    sinks: Vec<seatunnel_engine_core::connector_factory::SinkDeclaration>,
    on_sink_failure: String,
}

/// Extract every pipeline from the job config:
/// - `pipelines: [{source: {Plugin: {...}}, sinks: [{A: {...}}, ...]}, ...]`
///   for explicit multi-source / fan-out topologies;
/// - legacy shorthand `source: {Plugin: {...}}` + `sink:` (single block,
///   multi-key map or list — all entries become sinks of one pipeline).
fn extract_pipelines(
    config: &serde_json::Value,
    default_parallelism: usize,
) -> anyhow::Result<Vec<PipelineSpec>> {
    let default_policy = config
        .pointer("/env/on-sink-failure")
        .and_then(|v| v.as_str())
        .unwrap_or("fail")
        .to_string();

    let mut pipelines = Vec::new();
    if let Some(list) = config.get("pipelines").and_then(|v| v.as_array()) {
        if list.is_empty() {
            anyhow::bail!("'pipelines' section is empty");
        }
        for (idx, entry) in list.iter().enumerate() {
            let name = entry
                .get("name")
                .and_then(|v| v.as_str())
                .map(str::to_string)
                .unwrap_or_else(|| format!("p{}", idx));
            let parallelism = entry
                .get("parallelism")
                .and_then(|v| v.as_u64())
                .map(|v| (v as usize).max(1))
                .unwrap_or(default_parallelism);
            let source_section = entry
                .get("source")
                .ok_or_else(|| anyhow::anyhow!("pipeline '{}' has no source section", name))?;
            let (source_plugin, source_config) = first_block(source_section)
                .ok_or_else(|| anyhow::anyhow!("pipeline '{}' has an empty source section", name))?;
            let sink_section = entry
                .get("sinks")
                .or_else(|| entry.get("sink"))
                .ok_or_else(|| anyhow::anyhow!("pipeline '{}' has no sinks section", name))?;
            let sinks =
                seatunnel_engine_core::connector_factory::parse_sink_declarations(sink_section)?;
            let on_sink_failure = entry
                .get("on-sink-failure")
                .and_then(|v| v.as_str())
                .unwrap_or(&default_policy)
                .to_string();
            pipelines.push(PipelineSpec {
                name,
                parallelism,
                source_plugin,
                source_config,
                sinks,
                on_sink_failure,
            });
        }
        return Ok(pipelines);
    }

    // Legacy single-pipeline shorthand.
    let source_section = config
        .get("source")
        .ok_or_else(|| anyhow::anyhow!("job config has no source section"))?;
    let (source_plugin, source_config) = first_block(source_section)
        .ok_or_else(|| anyhow::anyhow!("job config has an empty source section"))?;
    let sink_section = config
        .get("sink")
        .ok_or_else(|| anyhow::anyhow!("job config has no sink section"))?;
    let sinks = seatunnel_engine_core::connector_factory::parse_sink_declarations(sink_section)?;
    Ok(vec![PipelineSpec {
        name: "pipeline".to_string(),
        parallelism: default_parallelism,
        source_plugin,
        source_config,
        sinks,
        on_sink_failure: default_policy,
    }])
}

/// First `{PluginName: {...}}` block of a section (map or single-item list).
fn first_block(section: &serde_json::Value) -> Option<(String, serde_json::Value)> {
    match section {
        serde_json::Value::Object(map) => map.iter().next().map(|(k, v)| (k.clone(), v.clone())),
        serde_json::Value::Array(items) => items.first().and_then(|item| {
            item.as_object()
                .and_then(|m| m.iter().next())
                .map(|(k, v)| (k.clone(), v.clone()))
        }),
        _ => None,
    }
}


impl JobCoordinator {
    pub fn new() -> Self {
        JobCoordinator {
            jobs: RwLock::new(HashMap::new()),
            all_tasks: RwLock::new(HashMap::new()),
            checkpoint_store: crate::checkpoint_store::MasterCheckpointStore::new(3),
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
        let pipelines = extract_pipelines(config, parallelism)?;
        let transforms = extract_transform_list(config);

        let summary = pipelines
            .iter()
            .map(|p| {
                format!(
                    "{}[{}: {} → {} sink(s), on-sink-failure={}]",
                    p.name,
                    p.parallelism,
                    p.source_plugin,
                    p.sinks.len(),
                    p.on_sink_failure
                )
            })
            .collect::<Vec<_>>()
            .join(", ");
        info!(
            "Compiling job {} '{}': checkpoint={}ms pipelines={}",
            job_id, job_name, checkpoint_interval, summary
        );

        // Build task descriptors — one per (pipeline, subtask), round-robin
        // over workers. Each descriptor carries its pipeline's source and
        // the FULL sink list (fan-out happens inside the task).
        let mut tasks = Vec::new();
        let mut task_no = 0usize;
        for (pipe_idx, pipe) in pipelines.iter().enumerate() {
            for i in 0..pipe.parallelism {
                let (worker_id, worker_addr) = &workers[task_no % workers.len()];
                let sinks_json: Vec<serde_json::Value> = pipe
                    .sinks
                    .iter()
                    .map(|sink| serde_json::json!({ "plugin": sink.plugin, "config": sink.config }))
                    .collect();
                tasks.push(TaskDescriptor {
                    task_id: format!("{}-p{}-{}", job_id, pipe_idx, i),
                    job_id: job_id.to_string(),
                    stage_id: format!("{}-p{}", job_id, pipe_idx),
                    task_name: format!("{}-{}-{}", job_name, pipe.name, i),
                    task_index: i as i32,
                    source_config_json: String::new(),
                    sink_config_json: String::new(),
                    parallelism: pipe.parallelism as i32,
                    config: HashMap::from([
                        ("worker_id".to_string(), worker_id.clone()),
                        ("worker_address".to_string(), worker_addr.clone()),
                        ("job.name".to_string(), job_name.to_string()),
                        ("pipeline.index".to_string(), pipe_idx.to_string()),
                        ("pipeline.name".to_string(), pipe.name.clone()),
                        ("pipeline.source.plugin".to_string(), pipe.source_plugin.clone()),
                        (
                            "pipeline.source.config".to_string(),
                            serde_json::to_string(&pipe.source_config)
                                .map_err(|e| anyhow::anyhow!("serialize source config: {}", e))?,
                        ),
                        (
                            "pipeline.sinks".to_string(),
                            serde_json::to_string(&sinks_json)
                                .map_err(|e| anyhow::anyhow!("serialize sinks: {}", e))?,
                        ),
                        (
                            "pipeline.on-sink-failure".to_string(),
                            pipe.on_sink_failure.clone(),
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
                task_no += 1;
            }
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
                    if desc
                        .config
                        .get("worker_id")
                        .map(|w| w == worker_id)
                        .unwrap_or(false)
                    {
                        out.push(desc.clone());
                    }
                }
            }
        }
        out
    }

    /// Tasks handed to a heartbeat-coming worker under the failover rule:
    /// pending (Created/Scheduled) tasks assigned to it, plus orphaned
    /// tasks (Deploying/Running) whose assigned worker is no longer in
    /// the live registry — reassigned to the requester. The second arm
    /// implements worker failover: a dead worker's tasks are taken over
    /// by any live worker.
    pub fn claim_tasks_for_worker(
        &self,
        worker_id: &str,
        worker_addr: &str,
        live_workers: &dyn Fn(&str) -> bool,
    ) -> Vec<TaskDescriptor> {
        let jobs = self.jobs.read();
        let mut all = self.all_tasks.write();
        let mut out = Vec::new();
        for job in jobs.values() {
            if job.state.is_terminal() {
                continue;
            }
            let task_ids: Vec<String> = job.tasks.keys().cloned().collect();
            for task_id in task_ids {
                let (_assigned, eligible) = {
                    let Some(info) = job.tasks.get(&task_id) else {
                        continue;
                    };
                    if matches!(info.state, JobState::Created | JobState::Scheduled) {
                        (info.worker_id.clone(), true)
                    } else if matches!(info.state, JobState::Deploying | JobState::Running) {
                        (
                            info.worker_id.clone(),
                            !live_workers(&info.worker_id),
                        )
                    } else {
                        continue;
                    }
                };
                if !eligible {
                    continue;
                }
                let Some(desc) = all.get_mut(&task_id) else {
                    continue;
                };
                let assigned_to_me = desc
                    .config
                    .get("worker_id")
                    .map(|w| w == worker_id)
                    .unwrap_or(false);
                if assigned_to_me {
                    // Regular pending handout.
                    out.push(desc.clone());
                } else if !live_workers(
                    desc.config.get("worker_id").map(String::as_str).unwrap_or(""),
                ) {
                    // Orphan takeover: reassign to the requester.
                    info!(
                        "Failover: task {} reassigned from dead worker '{}' to '{}'",
                        task_id,
                        desc.config.get("worker_id").cloned().unwrap_or_default(),
                        worker_id
                    );
                    desc.config.insert("worker_id".to_string(), worker_id.to_string());
                    desc.config
                        .insert("worker_address".to_string(), worker_addr.to_string());
                    out.push(desc.clone());
                }
            }
        }
        out
    }

    /// Evict a dead worker: mark its non-terminal tasks claimable again
    /// (the heartbeat claim rule takes over from here). Returns the
    /// affected task ids (for logging).
    pub fn evict_worker(&self, worker_id: &str) -> Vec<String> {
        let mut jobs = self.jobs.write();
        let mut affected = Vec::new();
        for job in jobs.values_mut() {
            if job.state.is_terminal() {
                continue;
            }
            for (task_id, info) in job.tasks.iter_mut() {
                if info.worker_id == worker_id
                    && matches!(info.state, JobState::Deploying | JobState::Running)
                {
                    // Deploying keeps the claim rule's orphan arm eligible;
                    // Running tasks must go back to a claimable state.
                    info.state = JobState::Deploying;
                    affected.push(task_id.clone());
                }
            }
        }
        affected
    }

    /// Mark tasks reported as running by a (re)registering worker so no
    /// other worker claims them, and return the subset this worker must
    /// STOP because they were reassigned elsewhere (fencing).
    pub fn register_running_tasks(
        &self,
        worker_id: &str,
        running_task_ids: &[String],
    ) -> Vec<String> {
        if running_task_ids.is_empty() {
            return Vec::new();
        }
        let jobs = self.jobs.read();
        let all = self.all_tasks.read();
        let mut preempted = Vec::new();
        for task_id in running_task_ids {
            let Some(job) = jobs.values().find(|j| j.tasks.contains_key(task_id)) else {
                continue;
            };
            let Some(info) = job.tasks.get(task_id) else {
                continue;
            };
            if info.worker_id == worker_id {
                continue; // still ours
            }
            // Verify the current owner via the descriptor (assignment
            // source of truth after failover reassignment).
            let current_owner = all
                .get(task_id)
                .and_then(|d| d.config.get("worker_id").cloned())
                .unwrap_or_default();
            if current_owner == worker_id {
                continue; // descriptor reassigned back to us
            }
            info!(
                "Fencing: task '{}' reported by worker '{}' but owned by '{}' — preempting",
                task_id, worker_id, current_owner
            );
            preempted.push(task_id.clone());
        }
        preempted
    }

    /// Serialize the full coordinator state (HA replication snapshot).
    pub async fn export_state(&self) -> serde_json::Value {
        use serde::Serialize;
        #[derive(Serialize)]
        struct TaskInfoDto {
            task_id: String,
            stage_id: String,
            worker_id: String,
            state: String,
            processed_records: u64,
            error: Option<String>,
        }
        #[derive(Serialize)]
        struct JobDto {
            job_id: String,
            job_name: String,
            state: String,
            parallelism: usize,
            start_time: i64,
            end_time: Option<i64>,
            error_message: Option<String>,
            checkpoint_interval_ms: u64,
            checkpoints_completed: u64,
            tasks: Vec<TaskInfoDto>,
        }
        let job_dtos: Vec<JobDto> = {
            let jobs = self.jobs.read();
            jobs.values()
                .map(|job| JobDto {
                    job_id: job.job_id.clone(),
                    job_name: job.job_name.clone(),
                    state: job.state.to_wire().to_string(),
                    parallelism: job.parallelism,
                    start_time: job.start_time,
                    end_time: job.end_time,
                    error_message: job.error_message.clone(),
                    checkpoint_interval_ms: job.checkpoint_interval_ms,
                    checkpoints_completed: job.checkpoints_completed,
                    tasks: job
                        .tasks
                        .values()
                        .map(|t| TaskInfoDto {
                            task_id: t.task_id.clone(),
                            stage_id: t.stage_id.clone(),
                            worker_id: t.worker_id.clone(),
                            state: t.state.to_wire().to_string(),
                            processed_records: t.processed_records,
                            error: t.error.clone(),
                        })
                        .collect(),
                })
                .collect()
        };
        let tasks_semantic: Vec<serde_json::Value> = {
            let all_tasks = self.all_tasks.read();
            all_tasks
                .values()
                .map(|d| {
                    serde_json::json!({
                        "task_id": d.task_id,
                        "job_id": d.job_id,
                        "stage_id": d.stage_id,
                        "task_name": d.task_name,
                        "task_index": d.task_index,
                        "parallelism": d.parallelism,
                        "config": d.config,
                    })
                })
                .collect()
        };
        let checkpoints = self.checkpoint_store.export().await;
        serde_json::json!({
            "jobs": job_dtos,
            "tasks": tasks_semantic,
            "checkpoints": checkpoints,
        })
    }

    /// Merge a replication snapshot: only fill gaps — local state always
    /// wins (the live master receives the authoritative reports).
    pub async fn import_state(&self, snapshot: &serde_json::Value) {
        use serde::Deserialize;
        #[derive(Deserialize)]
        struct TaskInfoDto {
            task_id: String,
            stage_id: String,
            worker_id: String,
            state: String,
            processed_records: u64,
            error: Option<String>,
        }
        #[derive(Deserialize)]
        struct JobDto {
            job_id: String,
            job_name: String,
            state: String,
            parallelism: usize,
            start_time: i64,
            end_time: Option<i64>,
            #[serde(default)]
            error_message: Option<String>,
            #[serde(default)]
            checkpoint_interval_ms: u64,
            #[serde(default)]
            checkpoints_completed: u64,
            tasks: Vec<TaskInfoDto>,
        }
        let Ok(jobs_dto) =
            serde_json::from_value::<Vec<JobDto>>(snapshot.get("jobs").cloned().unwrap_or_default())
        else {
            warn!("state import: malformed jobs section, skipped");
            return;
        };
        let mut imported = 0usize;
        {
            let mut jobs = self.jobs.write();
            for dto in jobs_dto {
                if jobs.contains_key(&dto.job_id) {
                    continue; // local wins
                }
                let tasks: HashMap<String, TaskInfo> = dto
                    .tasks
                    .into_iter()
                    .map(|t| {
                        (
                            t.task_id.clone(),
                            TaskInfo {
                                task_id: t.task_id,
                                stage_id: t.stage_id,
                                worker_id: t.worker_id,
                                state: JobState::from(t.state.as_str()),
                                processed_records: t.processed_records,
                                error: t.error,
                            },
                        )
                    })
                    .collect();
                jobs.insert(
                    dto.job_id.clone(),
                    RunningJob {
                        job_id: dto.job_id,
                        job_name: dto.job_name,
                        state: JobState::from(dto.state.as_str()),
                        parallelism: dto.parallelism,
                        tasks,
                        start_time: dto.start_time,
                        end_time: dto.end_time,
                        error_message: dto.error_message,
                        checkpoint_interval_ms: dto.checkpoint_interval_ms,
                        checkpoints_completed: dto.checkpoints_completed,
                    },
                );
                imported += 1;
            }
        }
        // Task descriptors (semantic fields only — enough for dispatch).
        let mut tasks_imported = 0usize;
        if let Some(tasks) = snapshot.get("tasks").and_then(|t| t.as_array()) {
            let mut all = self.all_tasks.write();
            for t in tasks {
                let Ok(task_id) =
                    serde_json::from_value::<String>(t.get("task_id").cloned().unwrap_or_default())
                else {
                    continue;
                };
                if all.contains_key(&task_id) {
                    continue;
                }
                let Ok(config) = serde_json::from_value::<HashMap<String, String>>(
                    t.get("config").cloned().unwrap_or_default(),
                ) else {
                    continue;
                };
                all.insert(
                    task_id.clone(),
                    TaskDescriptor {
                        task_id,
                        job_id: t.get("job_id").and_then(|v| v.as_str()).unwrap_or_default().to_string(),
                        stage_id: t.get("stage_id").and_then(|v| v.as_str()).unwrap_or_default().to_string(),
                        task_name: t.get("task_name").and_then(|v| v.as_str()).unwrap_or_default().to_string(),
                        task_index: t.get("task_index").and_then(|v| v.as_i64()).unwrap_or(0) as i32,
                        source_config_json: String::new(),
                        sink_config_json: String::new(),
                        parallelism: t.get("parallelism").and_then(|v| v.as_i64()).unwrap_or(1) as i32,
                        config,
                    },
                );
                tasks_imported += 1;
            }
        }
        // Master-backed checkpoint store.
        if let Some(checkpoints) = snapshot.get("checkpoints").cloned() {
            if let Ok(entries) = serde_json::from_value::<
                Vec<((String, String), crate::checkpoint_store::TaskCheckpoints)>,
            >(checkpoints)
            {
                self.checkpoint_store.import(entries).await;
            }
        }
        info!(
            "state import: {} job(s), {} task descriptor(s) merged",
            imported, tasks_imported
        );
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
            warn!(
                "status report for unknown task {} in job {}",
                task_id, job_id
            );
        }

        // A terminal job state (cancelled, failed, completed) is never
        // overwritten by late or in-flight task reports.
        if job.state.is_terminal() {
            return;
        }

        let total = job.tasks.len();
        let failed = job
            .tasks
            .values()
            .find(|t| matches!(t.state, JobState::Failed { .. }))
            .cloned();
        let running_or_more = job.tasks.values().any(|t| {
            matches!(
                t.state,
                JobState::Running | JobState::Deploying | JobState::Completed
            )
        });
        let completed = job
            .tasks
            .values()
            .filter(|t| t.state == JobState::Completed)
            .count();

        match failed {
            Some(f) => {
                let reason = f.error.clone().unwrap_or_else(|| "task failed".into());
                if !job.state.is_terminal() {
                    error!("Job {} failed: task {}: {}", job_id, f.task_id, reason);
                    job.state = JobState::Failed {
                        reason: reason.clone(),
                    };
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

    /// Newest checkpoint bytes for a task from the master store.
    pub async fn fetch_checkpoint(&self, job_id: &str, task_id: &str) -> Option<(u64, Vec<u8>)> {
        self.checkpoint_store.load_latest(job_id, task_id).await
    }

    /// The master-backed checkpoint store (async access).
    pub fn checkpoint_store(&self) -> &crate::checkpoint_store::MasterCheckpointStore {
        &self.checkpoint_store
    }

    /// Record a checkpoint reported by a worker.
    pub fn report_checkpoint(
        &self,
        job_id: &str,
        _task_id: &str,
        checkpoint_id: u64,
        success: bool,
    ) {
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
                tracing::warn!(
                    "Job {} checkpoint {} reported as failed",
                    job_id,
                    checkpoint_id
                );
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
                    full.insert(
                        "plugin_name".into(),
                        serde_json::Value::String(name.clone()),
                    );
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

/// Read `env.checkpoint.interval` from a job config. Handles both the nested
/// form (`env.checkpoint = { interval = N }`) and the dotted flat key
/// (`env."checkpoint.interval" = N`) produced by YAML/TOML parsers.
pub fn env_checkpoint_interval(config: &serde_json::Value) -> u64 {
    config
        .get("env")
        .and_then(|env| {
            // Nested shape.
            let nested = env.get("checkpoint").and_then(|c| match c {
                serde_json::Value::Number(n) => n.as_u64(),
                serde_json::Value::Object(m) => m.get("interval").and_then(|i| i.as_u64()),
                _ => None,
            });
            // Flat dotted key.
            let flat = env.get("checkpoint.interval").and_then(|v| v.as_u64());
            nested.or(flat)
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
    fn test_claim_orphan_reassigns_dead_worker_tasks() {
        let coordinator = JobCoordinator::new();
        let config = json!({
            "env": { "job.name": "j", "parallelism": 2 },
            "source": { "Fake": {} },
            "sink": { "Console": {} }
        });
        let (_job, tasks) = coordinator
            .compile_and_schedule("j1", "j", &config, None, &workers(2))
            .unwrap();
        // Dispatch both tasks to their assigned workers.
        let ids: Vec<String> = tasks.iter().map(|t| t.task_id.clone()).collect();
        coordinator.mark_tasks_dispatched(&ids, "worker-0");
        coordinator.mark_tasks_dispatched(&ids[1..], "worker-1");

        // worker-1 dies: its task becomes claimable by worker-0.
        let dead = "worker-1";
        let claimed = coordinator.claim_tasks_for_worker("worker-0", "127.0.0.1:9999", &|w| {
            w != dead
        });
        assert!(claimed
            .iter()
            .any(|t| t.config.get("worker_id").map(String::as_str) == Some("worker-0")));
        // The task descriptor was reassigned.
        let reassigned = claimed
            .iter()
            .find(|t| t.task_id == ids[1])
            .expect("orphan task claimed");
        assert_eq!(
            reassigned.config.get("worker_id").map(String::as_str),
            Some("worker-0")
        );
    }

    #[test]
    fn test_evict_worker_releases_running_tasks() {
        let coordinator = JobCoordinator::new();
        let config = json!({
            "env": { "parallelism": 1 },
            "source": { "Fake": {} },
            "sink": { "Console": {} }
        });
        let (_, tasks) = coordinator
            .compile_and_schedule("j2", "j", &config, None, &workers(1))
            .unwrap();
        let ids: Vec<String> = tasks.iter().map(|t| t.task_id.clone()).collect();
        coordinator.mark_tasks_dispatched(&ids, "worker-0");
        // Simulate RUNNING.
        coordinator.report_task_status("j2", &ids[0], "RUNNING", 5, None);

        let affected = coordinator.evict_worker("worker-0");
        assert_eq!(affected, vec![ids[0].clone()]);
        // Now claimable by a replacement worker.
        let claimed = coordinator.claim_tasks_for_worker("worker-1", "a", &|w| w != "worker-0");
        assert!(claimed.iter().any(|t| t.task_id == ids[0]));
    }

    #[test]
    fn test_fence_preempts_reassigned_tasks() {
        let coordinator = JobCoordinator::new();
        let config = json!({
            "env": { "parallelism": 1 },
            "source": { "Fake": {} },
            "sink": { "Console": {} }
        });
        let (_, tasks) = coordinator
            .compile_and_schedule("j3", "j", &config, None, &workers(1))
            .unwrap();
        let task_id = tasks[0].task_id.clone();
        coordinator.mark_tasks_dispatched(std::slice::from_ref(&task_id), "worker-0");
        // Failover: reassigned to worker-1.
        coordinator.evict_worker("worker-0");
        let _ = coordinator.claim_tasks_for_worker("worker-1", "a", &|w| w != "worker-0");
        coordinator.mark_tasks_dispatched(std::slice::from_ref(&task_id), "worker-1");

        // worker-0 comes back still running the task → must be fenced.
        let preempted =
            coordinator.register_running_tasks("worker-0", std::slice::from_ref(&task_id));
        assert_eq!(preempted, vec![task_id.clone()]);
        // worker-1's own report is not fenced.
        assert!(coordinator
            .register_running_tasks("worker-1", &[task_id])
            .is_empty());
    }

    #[tokio::test]
    async fn test_export_import_state_roundtrip() {
        let source = JobCoordinator::new();
        let config = json!({
            "env": { "parallelism": 1 },
            "source": { "Fake": {} },
            "sink": { "Console": {} }
        });
        source
            .compile_and_schedule("jx", "j", &config, None, &workers(1))
            .unwrap();
        source
            .checkpoint_store()
            .save("jx", "jx-p0-0", 7, b"cp-seven")
            .await;
        let snapshot = source.export_state().await;

        // Empty standby imports it.
        let standby = JobCoordinator::new();
        standby.import_state(&snapshot).await;
        let (id, data) = standby
            .checkpoint_store()
            .load_latest("jx", "jx-p0-0")
            .await
            .unwrap();
        assert_eq!((id, data.as_slice()), (7, b"cp-seven".as_slice()));
        // Imported job schedules tasks.
        let claimed = standby.claim_tasks_for_worker("worker-0", "a", &|_| true);
        assert!(!claimed.is_empty());

        // Local state wins: importing again does not duplicate.
        standby.import_state(&snapshot).await;
        let count = standby
            .export_state()
            .await
            .get("jobs")
            .and_then(|j| j.as_array().map(|a| a.len()))
            .unwrap_or(0);
        assert_eq!(count, 1);
    }

    #[test]
    fn test_compile_multi_pipeline_with_fanout() {
        let coordinator = JobCoordinator::new();
        let config = json!({
            "env": { "job.name": "multi", "parallelism": 2, "on-sink-failure": "isolate" },
            "pipelines": [
                {
                    "name": "cdc-fanout",
                    "parallelism": 1,
                    "source": { "MySQL-CDC": { "database-name": "s", "table-name": "t" } },
                    "sinks": [
                        { "Kafka": { "topic": "a" } },
                        { "JDBC": { "url": "jdbc:mysql://x" } }
                    ]
                },
                {
                    // no name → p1; default parallelism from env (2)
                    "source": { "JDBC": { "url": "jdbc:mysql://y" } },
                    "sink": { "Console": {} }
                }
            ]
        });

        let (_job, tasks) = coordinator
            .compile_and_schedule("j2", "multi", &config, None, &workers(2))
            .unwrap();
        // pipeline 0: 1 subtask; pipeline 1: 2 subtasks.
        assert_eq!(tasks.len(), 3);
        let t = &tasks[0];
        assert_eq!(t.task_id, "j2-p0-0");
        assert_eq!(t.config.get("pipeline.name").unwrap(), "cdc-fanout");
        assert_eq!(t.config.get("pipeline.source.plugin").unwrap(), "MySQL-CDC");
        assert_eq!(t.config.get("pipeline.on-sink-failure").unwrap(), "isolate");
        let sinks: serde_json::Value =
            serde_json::from_str(t.config.get("pipeline.sinks").unwrap()).unwrap();
        assert_eq!(sinks.as_array().unwrap().len(), 2);
        assert_eq!(sinks[0]["plugin"], "Kafka");
        assert_eq!(sinks[1]["plugin"], "JDBC");

        let t1 = &tasks[1];
        assert_eq!(t1.task_id, "j2-p1-0");
        assert_eq!(t1.parallelism, 2);
        assert_eq!(t1.task_index, 0);
        let t2 = &tasks[2];
        assert_eq!(t2.task_index, 1);
        // Default policy inherited from env.
        assert_eq!(t2.config.get("pipeline.on-sink-failure").unwrap(), "isolate");
    }

    #[test]
    fn test_compile_legacy_sink_list() {
        let coordinator = JobCoordinator::new();
        let config = json!({
            "env": { "job.name": "legacy", "parallelism": 1 },
            "source": { "Fake": { "row.num": 1 } },
            "sink": [
                { "Console": {} },
                { "Console": { "prefix": "x" } }
            ]
        });
        let (_job, tasks) = coordinator
            .compile_and_schedule("j3", "legacy", &config, None, &workers(1))
            .unwrap();
        let sinks: serde_json::Value =
            serde_json::from_str(tasks[0].config.get("pipeline.sinks").unwrap()).unwrap();
        assert_eq!(sinks.as_array().unwrap().len(), 2);
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
        assert_eq!(t0.task_id, "j1-p0-0");
        assert_eq!(
            t0.config.get("pipeline.source.plugin").unwrap(),
            "MySQL-CDC"
        );
        assert_eq!(t0.config.get("checkpoint.interval").unwrap(), "10000");
        let src: serde_json::Value =
            serde_json::from_str(t0.config.get("pipeline.source.config").unwrap()).unwrap();
        assert_eq!(src["hostname"], "db");
        let sinks: serde_json::Value =
            serde_json::from_str(t0.config.get("pipeline.sinks").unwrap()).unwrap();
        assert_eq!(sinks[0]["plugin"], "Kafka");
        assert_eq!(sinks[0]["config"]["topic"], "out");

        // Round-robin placement across three workers.
        assert_eq!(tasks[0].config.get("worker_id").unwrap(), "worker-0");
        assert_eq!(tasks[1].config.get("worker_id").unwrap(), "worker-1");

        // Dispatch dedup: pending once, gone after marking dispatched.
        let pending = coordinator.get_pending_tasks_for_worker("worker-0");
        assert_eq!(pending.len(), 1);
        coordinator.mark_tasks_dispatched(
            &pending
                .iter()
                .map(|t| t.task_id.clone())
                .collect::<Vec<_>>(),
            "worker-0",
        );
        assert!(coordinator
            .get_pending_tasks_for_worker("worker-0")
            .is_empty());
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
            coordinator.mark_tasks_dispatched(std::slice::from_ref(&t.task_id), "worker-0");
        }
        let job = coordinator.get_job(&job_id).unwrap();
        assert_eq!(job.state, JobState::Deploying);

        for t in &tasks {
            coordinator.report_task_status(&job_id, &t.task_id, "RUNNING", 5, None);
        }
        assert_eq!(
            coordinator.get_job(&job_id).unwrap().state,
            JobState::Running
        );

        for t in &tasks {
            coordinator.report_task_status(&job_id, &t.task_id, "COMPLETED", 50, None);
        }
        assert_eq!(
            coordinator.get_job(&job_id).unwrap().state,
            JobState::Completed
        );
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
        assert_eq!(
            env_parallelism(&json!({"env":{"parallelism":{"default":3}}})),
            3
        );
        assert_eq!(env_parallelism(&json!({})), 1);
        assert_eq!(
            env_checkpoint_interval(&json!({"env":{"checkpoint":{"interval":5000}}})),
            5000
        );
        assert_eq!(
            env_checkpoint_interval(&json!({"env":{"checkpoint":12345}})),
            12345
        );
        // Flat dotted key produced by YAML/TOML parsers.
        assert_eq!(
            env_checkpoint_interval(&json!({"env":{"checkpoint.interval":7000}})),
            7000
        );
        assert_eq!(
            env_checkpoint_interval(&json!({})),
            DEFAULT_CHECKPOINT_INTERVAL_MS
        );
    }
}
