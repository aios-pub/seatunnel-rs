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
use std::sync::atomic::{AtomicU64, Ordering};

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
    /// Epoch-ms of the most recent record (0 = none yet), from heartbeats.
    pub last_record_at: i64,
    /// Most recent completed checkpoint id and state size (heartbeats).
    pub last_checkpoint_id: u64,
    pub last_checkpoint_size: u64,
    /// Bounded tail of task log lines shipped by worker heartbeats.
    pub logs: std::collections::VecDeque<String>,
}


// ---------------------------------------------------------------------------
// Serializable state records: shared by HA export/import and the Raft
// command log (prost message types do not derive serde).
// ---------------------------------------------------------------------------

/// Serializable `TaskInfo` (running-job task state).
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct TaskInfoDto {
    pub task_id: String,
    pub stage_id: String,
    pub worker_id: String,
    pub state: String,
    pub processed_records: u64,
    #[serde(default)]
    pub error: Option<String>,
}

/// Serializable `RunningJob` (without the derived/volatile bookkeeping).
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct JobDto {
    pub job_id: String,
    pub job_name: String,
    pub state: String,
    pub parallelism: usize,
    pub start_time: i64,
    pub end_time: Option<i64>,
    #[serde(default)]
    pub error_message: Option<String>,
    #[serde(default)]
    pub checkpoint_interval_ms: u64,
    #[serde(default)]
    pub checkpoints_completed: u64,
    /// Master-assigned checkpoint id counter — a standby importing this
    /// snapshot must continue after it, never behind.
    #[serde(default = "one")]
    pub next_checkpoint_id: u64,
    pub tasks: Vec<TaskInfoDto>,
}

/// Serializable `TaskDescriptor` payload (the dispatchable plan).
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct TaskDto {
    pub task_id: String,
    pub job_id: String,
    pub stage_id: String,
    pub task_name: String,
    pub task_index: i32,
    pub parallelism: i32,
    pub config: HashMap<String, String>,
}

impl From<&TaskDescriptor> for TaskDto {
    fn from(d: &TaskDescriptor) -> Self {
        TaskDto {
            task_id: d.task_id.clone(),
            job_id: d.job_id.clone(),
            stage_id: d.stage_id.clone(),
            task_name: d.task_name.clone(),
            task_index: d.task_index,
            parallelism: d.parallelism,
            config: d.config.clone(),
        }
    }
}

fn one() -> u64 {
    1
}


// ---------------------------------------------------------------------------
// Raft command log: every durable coordinator mutation is one Command.
// The leader computes non-deterministic inputs (worker snapshot, plan,
// timestamps) before proposing; apply() replays deterministically.
// ---------------------------------------------------------------------------

/// A durable coordinator mutation (Raft AppData).
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub enum Command {
    /// Install a fully compiled job plan (leader pre-computes placement).
    SubmitJob { job: JobDto, descriptors: Vec<TaskDto> },
    CancelJob { job_id: String, at_ms: i64 },
    /// A task lifecycle transition rolled up to job state.
    TaskStatus {
        job_id: String,
        task_id: String,
        worker_id: String,
        state: String,
        records: u64,
        error: Option<String>,
    },
    /// Tasks handed to a worker (never re-dispatched).
    MarkDispatched {
        task_ids: Vec<String>,
        worker_id: String,
    },
    /// A worker left the cluster (crash or graceful unregister): its
    /// non-terminal tasks become claimable again.
    EvictWorker { worker_id: String },
    /// Re-attach still-assigned tasks to a returning worker.
    AdoptTasks {
        worker_id: String,
        task_ids: Vec<String>,
    },
    /// Allocate a coordinated checkpoint id for a pipeline and open the
    /// pending set (participants derived from applied state).
    CheckpointTriggered { job_id: String, stage_id: String, at_ms: i64 },
    /// Resolve a coordinated checkpoint (complete or abort): counters,
    /// per-task ids and the worker resolution outbox.
    CheckpointResolved {
        job_id: String,
        stage_id: String,
        checkpoint_id: u64,
        completed: bool,
        participants: Vec<(String, String)>,
    },
    /// Observe a fencing term (legacy merge path; Raft term ratchets too).
    ObserveTerm { term: u64 },
}

/// Result of applying one command (Raft AppDataResponse).
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub enum CommandResult {
    Ok,
    /// The id allocated by `CheckpointTriggered`.
    CheckpointId(u64),
}

/// Retained task log lines per task (bounded memory on the master).
const TASK_LOG_RETAIN: usize = 500;

impl TaskInfo {
    pub fn new(task_id: String, stage_id: String, worker_id: String) -> Self {
        TaskInfo {
            task_id,
            stage_id,
            worker_id,
            state: JobState::Created,
            processed_records: 0,
            error: None,
            last_record_at: 0,
            last_checkpoint_id: 0,
            last_checkpoint_size: 0,
            logs: std::collections::VecDeque::new(),
        }
    }

    fn append_logs(&mut self, lines: Vec<String>) {
        for line in lines {
            self.logs.push_back(line);
        }
        let excess = self.logs.len().saturating_sub(TASK_LOG_RETAIN);
        if excess > 0 {
            self.logs.drain(..excess);
        }
    }
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
    /// Coordinated-checkpoint id counter (master-assigned). Exported with
    /// the HA snapshot so a failover NEVER rewinds ids — a sink that sees
    /// a repeated checkpoint id cannot fence zombie transactions.
    pub next_checkpoint_id: u64,
    /// Epoch-ms of the last coordinated trigger (volatile, not exported).
    pub last_checkpoint_trigger_ms: i64,
}

/// Job coordinator managing the lifecycle of all submitted jobs.
pub struct JobCoordinator {
    jobs: RwLock<HashMap<String, RunningJob>>,
    /// Full descriptors by task_id — handed to workers at dispatch time.
    all_tasks: RwLock<HashMap<String, TaskDescriptor>>,
    /// Master-backed shared checkpoint store (storage type = master).
    checkpoint_store: crate::checkpoint_store::MasterCheckpointStore,
    /// Fencing term (monotonic). Workers reject instructions from masters
    /// with a lower term, so a deposed master cannot disturb tasks owned
    /// by its successor. Replicated with the HA snapshot; becomes the
    /// Raft leader term once consensus-based HA lands.
    term: AtomicU64,
    /// Coordinated checkpoints in flight, keyed by (job_id, stage_id):
    /// one per pipeline. The master is the checkpoint driver — it assigns
    /// the id, collects every participating task's prepare, then resolves
    /// (complete → workers run 2PC phase 2, or abort).
    pending_checkpoints: RwLock<HashMap<(String, String), PendingCheckpoint>>,
    /// Resolution events waiting for the owning worker's next heartbeat,
    /// keyed by worker_id.
    checkpoint_outbox: std::sync::Mutex<HashMap<String, Vec<seatunnel_engine_comm::CheckpointResolution>>>,
    /// Preemption fence events (task ids a worker must stop because
    /// another worker owns/executes them), delivered via heartbeats.
    preempt_outbox: std::sync::Mutex<HashMap<String, Vec<String>>>,
}

/// One in-flight coordinated checkpoint for a pipeline.
#[derive(Debug, Clone)]
struct PendingCheckpoint {
    checkpoint_id: u64,
    stage_id: String,
    triggered_at_ms: i64,
    /// task_id → owning worker (for resolution delivery).
    participants: HashMap<String, String>,
    /// Tasks whose trigger was already handed to their worker's
    /// heartbeat. Delivery is per-worker and must survive the interval
    /// gate: a worker that heartbeats late still gets its trigger.
    delivered: std::collections::HashSet<String>,
    /// Tasks that still owe a prepare (or a failure).
    awaiting: Vec<String>,
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
            let (source_plugin, source_config) = first_block(source_section).ok_or_else(|| {
                anyhow::anyhow!("pipeline '{}' has an empty source section", name)
            })?;
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
            // Term 0 is reserved for "worker has not seen a master yet";
            // masters always operate at term >= 1.
            term: AtomicU64::new(1),
            pending_checkpoints: RwLock::new(HashMap::new()),
            checkpoint_outbox: std::sync::Mutex::new(HashMap::new()),
            preempt_outbox: std::sync::Mutex::new(HashMap::new()),
        }
    }

    /// Take the preemption fence events queued for a worker's heartbeat.
    pub fn drain_preemptions(&self, worker_id: &str) -> Vec<String> {
        self.preempt_outbox
            .lock()
            .unwrap()
            .remove(worker_id)
            .unwrap_or_default()
    }

    /// Queue a preemption (fence) for a worker.
    pub fn queue_preemption(&self, worker_id: &str, task_id: &str) {
        self.preempt_outbox
            .lock()
            .unwrap()
            .entry(worker_id.to_string())
            .or_default()
            .push(task_id.to_string());
    }

    /// Current fencing term.
    pub fn term(&self) -> u64 {
        self.term.load(Ordering::SeqCst)
    }

    /// Ratchet the term up to an externally observed value (replication
    /// snapshot, worker report). Never decreases. Returns the current term.
    pub fn observe_term(&self, seen: u64) -> u64 {
        let mut current = self.term.load(Ordering::SeqCst);
        while seen > current {
            match self.term.compare_exchange(
                current,
                seen,
                Ordering::SeqCst,
                Ordering::SeqCst,
            ) {
                Ok(_) => return seen,
                Err(actual) => current = actual,
            }
        }
        current
    }

    /// Compile a job config into a plan WITHOUT installing it: the pure
    /// leader-side half of submission (consensus mode proposes the plan as
    /// `Command::SubmitJob`).
    pub fn plan_job(
        &self,
        job_id: &str,
        job_name: &str,
        config: &serde_json::Value,
        parallelism_override: Option<usize>,
        workers: &[(String, String)],
    ) -> anyhow::Result<(JobDto, Vec<TaskDto>, Vec<TaskDescriptor>)> {
        let (job_dto, dtos, tasks) = self.compile_plan_inner(
            job_id,
            job_name,
            config,
            parallelism_override,
            workers,
        )?;
        Ok((job_dto, dtos, tasks))
    }

    /// Compile a job config into chained task descriptors and register the job.
    ///
    /// Fails when no worker has registered yet — silently queueing would leave
    /// users staring at a SCHEDULED job forever.
    fn compile_plan_inner(
        &self,
        job_id: &str,
        job_name: &str,
        config: &serde_json::Value,
        parallelism_override: Option<usize>,
        workers: &[(String, String)],
    ) -> anyhow::Result<(JobDto, Vec<TaskDto>, Vec<TaskDescriptor>)> {
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
                        (
                            "pipeline.source.plugin".to_string(),
                            pipe.source_plugin.clone(),
                        ),
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
                    ..TaskInfo::new(t.task_id.clone(), t.stage_id.clone(), String::new())
                },
            );
        }

        let job_dto = JobDto {
            job_id: job_id.to_string(),
            job_name: job_name.to_string(),
            state: JobState::Scheduled.to_wire().to_string(),
            parallelism,
            start_time: now,
            end_time: None,
            error_message: None,
            checkpoint_interval_ms: checkpoint_interval,
            checkpoints_completed: 0,
            next_checkpoint_id: 1,
            tasks: task_infos
                .values()
                .map(|t| TaskInfoDto {
                    task_id: t.task_id.clone(),
                    stage_id: t.stage_id.clone(),
                    worker_id: t.worker_id.clone(),
                    state: t.state.to_wire().to_string(),
                    processed_records: t.processed_records,
                    error: None,
                })
                .collect(),
        };
        let dtos: Vec<TaskDto> = tasks.iter().map(TaskDto::from).collect();
        Ok((job_dto, dtos, tasks))
    }

    /// Direct-mode submission: plan + install in place.
    pub fn compile_and_install(
        &self,
        job_id: &str,
        job_name: &str,
        config: &serde_json::Value,
        parallelism_override: Option<usize>,
        workers: &[(String, String)],
    ) -> anyhow::Result<(String, Vec<TaskDescriptor>)> {
        let (job_dto, dtos, _tasks) =
            self.compile_plan_inner(job_id, job_name, config, parallelism_override, workers)?;
        let tasks: Vec<TaskDescriptor> = dtos
            .iter()
            .map(|d| TaskDescriptor {
                task_id: d.task_id.clone(),
                job_id: d.job_id.clone(),
                stage_id: d.stage_id.clone(),
                task_name: d.task_name.clone(),
                task_index: d.task_index,
                source_config_json: String::new(),
                sink_config_json: String::new(),
                parallelism: d.parallelism,
                config: d.config.clone(),
            })
            .collect();
        self.install_job(job_dto, dtos);
        Ok((job_id.to_string(), tasks))
    }

    /// Install a compiled plan (Raft `Command::SubmitJob` lands here).
    /// The plan is fully computed by the leader — apply is deterministic.
    pub fn install_job(&self, job: JobDto, descriptors: Vec<TaskDto>) {
        let running = RunningJob {
            job_id: job.job_id.clone(),
            job_name: job.job_name,
            state: JobState::from(job.state.as_str()),
            parallelism: job.parallelism,
            tasks: job
                .tasks
                .into_iter()
                .map(|t| {
                    (
                        t.task_id.clone(),
                        TaskInfo {
                            state: JobState::from(t.state.as_str()),
                            processed_records: t.processed_records,
                            error: t.error,
                            ..TaskInfo::new(t.task_id.clone(), t.stage_id.clone(), t.worker_id.clone())
                        },
                    )
                })
                .collect(),
            start_time: job.start_time,
            end_time: job.end_time,
            error_message: job.error_message,
            checkpoint_interval_ms: job.checkpoint_interval_ms,
            checkpoints_completed: job.checkpoints_completed,
            next_checkpoint_id: job.next_checkpoint_id,
            last_checkpoint_trigger_ms: 0,
        };
        self.jobs.write().insert(running.job_id.clone(), running);
        let mut all = self.all_tasks.write();
        for d in descriptors {
            all.insert(
                d.task_id.clone(),
                TaskDescriptor {
                    task_id: d.task_id,
                    job_id: d.job_id,
                    stage_id: d.stage_id,
                    task_name: d.task_name,
                    task_index: d.task_index,
                    source_config_json: String::new(),
                    sink_config_json: String::new(),
                    parallelism: d.parallelism,
                    config: d.config,
                },
            );
        }
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

    /// Read-only claim decision: tasks this heartbeat-coming worker may
    /// take — pending (Created/Scheduled) tasks assigned to it, plus
    /// orphaned tasks (Deploying/Running) whose assigned worker is no
    /// longer in the live registry. Mutations (reassignment + Deploying)
    /// happen in [`Self::mark_tasks_dispatched`], reached through a Raft
    /// `Command::MarkDispatched` in consensus mode.
    pub fn claim_tasks_for_worker(
        &self,
        worker_id: &str,
        _worker_addr: &str,
        live_workers: &dyn Fn(&str) -> bool,
    ) -> Vec<TaskDescriptor> {
        let jobs = self.jobs.read();
        let all = self.all_tasks.read();
        let mut out = Vec::new();
        for job in jobs.values() {
            if job.state.is_terminal() {
                continue;
            }
            for info in job.tasks.values() {
                let eligible = match info.state {
                    JobState::Created | JobState::Scheduled => {
                        info.worker_id == worker_id
                    }
                    JobState::Deploying | JobState::Running => !live_workers(&info.worker_id),
                    _ => false,
                };
                if !eligible {
                    continue;
                }
                if let Some(desc) = all.get(&info.task_id) {
                    if desc
                        .config
                        .get("worker_id")
                        .map(|w| w == worker_id)
                        .unwrap_or(false)
                        || !live_workers(
                            desc.config
                                .get("worker_id")
                                .map(String::as_str)
                                .unwrap_or(""),
                        )
                    {
                        if !live_workers(&info.worker_id) && info.worker_id != worker_id {
                            info!(
                                "Failover: task {} of dead worker '{}' claimable by '{}'",
                                info.task_id, info.worker_id, worker_id
                            );
                        }
                        out.push(desc.clone());
                    }
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

    /// Tasks a (re)registering worker reports as still running.
    ///
    /// Adopt-first (the Java engine's re-attach lesson): a task whose
    /// assignment still points at this worker is ADOPTED — re-marked
    /// `Running` so no other worker claims it during the reconnect
    /// window. Tasks reassigned to ANOTHER worker are returned for local
    /// preemption (fencing against double execution).
    ///
    /// Read-only classification; the adopt mutation goes through
    /// [`Self::adopt_tasks`] (or a Raft `Command::AdoptTasks`).
    pub fn classify_running_tasks(
        &self,
        worker_id: &str,
        running_task_ids: &[String],
    ) -> (Vec<String>, Vec<String>) {
        if running_task_ids.is_empty() {
            return (Vec::new(), Vec::new());
        }
        let jobs = self.jobs.read();
        let all = self.all_tasks.read();
        let mut adopt = Vec::new();
        let mut preempted = Vec::new();
        for task_id in running_task_ids {
            let Some(job) = jobs.values().find(|j| j.tasks.contains_key(task_id)) else {
                continue;
            };
            let Some(info) = job.tasks.get(task_id) else {
                continue;
            };
            if info.worker_id == worker_id {
                adopt.push(task_id.clone());
                continue; // still ours
            }
            // The descriptor's worker_id is the assignment source of truth
            // (failover reassignment mutates it in place).
            let current_owner = all
                .get(task_id)
                .and_then(|d| d.config.get("worker_id").cloned())
                .unwrap_or_default();
            if current_owner == worker_id || current_owner.is_empty() {
                adopt.push(task_id.clone());
            } else {
                info!(
                    "Fencing: task '{}' reported by worker '{}' but owned by '{}' — preempting",
                    task_id, worker_id, current_owner
                );
                preempted.push(task_id.clone());
            }
        }
        (adopt, preempted)
    }

    /// Convenience for the direct (non-Raft) path: classify + adopt,
    /// returning the preempt list. The Raft path proposes AdoptTasks.
    pub fn register_running_tasks(
        &self,
        worker_id: &str,
        running_task_ids: &[String],
    ) -> Vec<String> {
        let (adopt, preempted) = self.classify_running_tasks(worker_id, running_task_ids);
        self.adopt_tasks(worker_id, &adopt);
        preempted
    }

    /// Adopt tasks for a worker: still-assigned tasks are re-marked
    /// Running for it (Raft `Command::AdoptTasks` lands here).
    pub fn adopt_tasks(&self, worker_id: &str, task_ids: &[String]) {
        if task_ids.is_empty() {
            return;
        }
        let mut jobs = self.jobs.write();
        for task_id in task_ids {
            for job in jobs.values_mut() {
                if let Some(info) = job.tasks.get_mut(task_id) {
                    if matches!(
                        info.state,
                        JobState::Created | JobState::Scheduled | JobState::Deploying
                    ) {
                        info.state = JobState::Running;
                        info.worker_id = worker_id.to_string();
                    }
                    break;
                }
            }
        }
    }

    /// Serialize the full coordinator state (HA snapshot / Raft state
    /// machine snapshot — one and the same thing).
    pub async fn export_state(&self) -> serde_json::Value {
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
                    next_checkpoint_id: job.next_checkpoint_id,
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
            "term": self.term(),
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
        fn one() -> u64 {
            1
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
            /// Checkpoint id counter from the exporting master; take the
            /// max with local so ids never rewind after failover.
            #[serde(default = "one")]
            next_checkpoint_id: u64,
            tasks: Vec<TaskInfoDto>,
        }
        // The fencing term never regresses: a standby that imported a
        // higher-term snapshot (a successor took over) keeps that term.
        if let Some(seen) = snapshot.get("term").and_then(|v| v.as_u64()) {
            self.observe_term(seen);
        }
        let Ok(jobs_dto) = serde_json::from_value::<Vec<JobDto>>(
            snapshot.get("jobs").cloned().unwrap_or_default(),
        ) else {
            warn!("state import: malformed jobs section, skipped");
            return;
        };
        let mut imported = 0usize;
        {
            let mut jobs = self.jobs.write();
            for dto in jobs_dto {
                if let Some(existing) = jobs.get_mut(&dto.job_id) {
                    // The checkpoint id counter never rewinds, even when
                    // the local job record is otherwise kept (local wins).
                    existing.next_checkpoint_id = existing
                        .next_checkpoint_id
                        .max(dto.next_checkpoint_id);
                    continue;
                }
                let tasks: HashMap<String, TaskInfo> = dto
                    .tasks
                    .into_iter()
                    .map(|t| {
                        (
                            t.task_id.clone(),
                            TaskInfo {
                                state: JobState::from(t.state.as_str()),
                                processed_records: t.processed_records,
                                error: t.error,
                                ..TaskInfo::new(t.task_id.clone(), t.stage_id.clone(), t.worker_id.clone())
                            },
                        )
                    })
                    .collect();
                jobs.insert(
                    dto.job_id.clone(),
                    RunningJob {
                        next_checkpoint_id: dto.next_checkpoint_id,
                        last_checkpoint_trigger_ms: 0,
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
                        job_id: t
                            .get("job_id")
                            .and_then(|v| v.as_str())
                            .unwrap_or_default()
                            .to_string(),
                        stage_id: t
                            .get("stage_id")
                            .and_then(|v| v.as_str())
                            .unwrap_or_default()
                            .to_string(),
                        task_name: t
                            .get("task_name")
                            .and_then(|v| v.as_str())
                            .unwrap_or_default()
                            .to_string(),
                        task_index: t.get("task_index").and_then(|v| v.as_i64()).unwrap_or(0)
                            as i32,
                        source_config_json: String::new(),
                        sink_config_json: String::new(),
                        parallelism: t.get("parallelism").and_then(|v| v.as_i64()).unwrap_or(1)
                            as i32,
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

    /// Replace the full state from a snapshot (Raft snapshot install).
    /// Unlike [`Self::import_state`] (merge/gap-fill for the legacy
    /// warm-standby path), a snapshot is authoritative: local state is
    /// discarded. `last_applied` bookkeeping stays with the caller.
    pub async fn replace_state(&self, snapshot: &serde_json::Value) {
        self.jobs.write().clear();
        self.all_tasks.write().clear();
        self.pending_checkpoints.write().clear();
        self.checkpoint_outbox.lock().unwrap().clear();
        if let Some(term) = snapshot.get("term").and_then(|v| v.as_u64()) {
            self.observe_term(term);
        }
        // Reuse the merge importer now that local state is empty.
        self.import_state(snapshot).await;
    }

/// Deterministic command application — the Raft state machine core.
    /// Every input the command needs (plans, timestamps, resolutions) was
    /// computed by the leader; replay on any node yields the same state.
    pub fn apply_command(&self, cmd: &Command) -> CommandResult {
        match cmd {
            Command::SubmitJob { job, descriptors } => {
                self.install_job(job.clone(), descriptors.clone());
                CommandResult::Ok
            }
            Command::CancelJob { job_id, at_ms } => {
                let cancelled = {
                    let mut jobs = self.jobs.write();
                    match jobs.get_mut(job_id) {
                        Some(job) if !job.state.is_terminal() => {
                            job.state = JobState::Cancelled;
                            job.end_time = Some(*at_ms);
                            true
                        }
                        _ => false,
                    }
                };
                if cancelled {
                    info!("Job {} cancelled by user", job_id);
                    self.drop_pending_checkpoints(job_id);
                }
                CommandResult::Ok
            }
            Command::TaskStatus {
                job_id,
                task_id,
                worker_id,
                state,
                records,
                error,
            } => {
                self.report_task_status_from(
                    worker_id, job_id, task_id, state, *records, error.clone(),
                );
                CommandResult::Ok
            }
            Command::MarkDispatched { task_ids, worker_id } => {
                self.mark_tasks_dispatched(task_ids, worker_id);
                CommandResult::Ok
            }
            Command::EvictWorker { worker_id } => {
                self.evict_worker(worker_id);
                CommandResult::Ok
            }
            Command::AdoptTasks { worker_id, task_ids } => {
                self.adopt_tasks(worker_id, task_ids);
                CommandResult::Ok
            }
            Command::CheckpointTriggered {
                job_id,
                stage_id,
                at_ms,
            } => CommandResult::CheckpointId(
                self.apply_checkpoint_triggered(job_id, stage_id, *at_ms),
            ),
            Command::CheckpointResolved {
                job_id,
                stage_id,
                checkpoint_id,
                completed,
                participants,
            } => {
                self.apply_checkpoint_resolved(
                    job_id,
                    stage_id,
                    *checkpoint_id,
                    *completed,
                    participants,
                );
                CommandResult::Ok
            }
            Command::ObserveTerm { term } => {
                self.observe_term(*term);
                CommandResult::Ok
            }
        }
    }

    /// Deterministic `CheckpointTriggered`: allocate the id from the job's
    /// counter and open the pending set from applied state (Running tasks
    /// of the pipeline). Participants derive from state, not from the
    /// proposal, so replay is identical on every node.
    fn apply_checkpoint_triggered(&self, job_id: &str, stage_id: &str, at_ms: i64) -> u64 {
        let mut jobs = self.jobs.write();
        let Some(job) = jobs.get_mut(job_id) else {
            return 0;
        };
        if job.state != JobState::Running {
            return 0;
        }
        let checkpoint_id = job.next_checkpoint_id;
        job.next_checkpoint_id = job.next_checkpoint_id.saturating_add(1);
        job.last_checkpoint_trigger_ms = at_ms;
        let mut participants = HashMap::new();
        for info in job.tasks.values() {
            if info.state == JobState::Running && info.stage_id == stage_id {
                participants.insert(info.task_id.clone(), info.worker_id.clone());
            }
        }
        let awaiting: Vec<String> = participants.keys().cloned().collect();
        info!(
            "Job {} pipeline {}: coordinated checkpoint {} triggered on {} task(s)",
            job_id,
            stage_id,
            checkpoint_id,
            awaiting.len()
        );
        drop(jobs);
        self.pending_checkpoints.write().insert(
            (job_id.to_string(), stage_id.to_string()),
            PendingCheckpoint {
                checkpoint_id,
                stage_id: stage_id.to_string(),
                triggered_at_ms: at_ms,
                participants,
                delivered: std::collections::HashSet::new(),
                awaiting,
            },
        );
        checkpoint_id
    }

    /// Deterministic `CheckpointResolved`: close the pending, bump
    /// counters, surface ids on participants, queue worker events.
    fn apply_checkpoint_resolved(
        &self,
        job_id: &str,
        stage_id: &str,
        checkpoint_id: u64,
        completed: bool,
        participants: &[(String, String)],
    ) {
        self.pending_checkpoints
            .write()
            .remove(&(job_id.to_string(), stage_id.to_string()));
        if completed {
            let mut jobs = self.jobs.write();
            if let Some(job) = jobs.get_mut(job_id) {
                job.checkpoints_completed += 1;
                for (task_id, _) in participants {
                    if let Some(info) = job.tasks.get_mut(task_id) {
                        if checkpoint_id > info.last_checkpoint_id {
                            info.last_checkpoint_id = checkpoint_id;
                            info.last_checkpoint_size = 0;
                        }
                    }
                }
            }
        }
        let mut outbox = self.checkpoint_outbox.lock().unwrap();
        for (task_id, worker_id) in participants {
            outbox
                .entry(worker_id.clone())
                .or_default()
                .push(seatunnel_engine_comm::CheckpointResolution {
                    task_id: task_id.clone(),
                    checkpoint_id,
                    completed,
                });
        }
    }

    /// Mark tasks as handed to a worker so they are never re-dispatched.

    pub fn mark_tasks_dispatched(&self, task_ids: &[String], worker_id: &str) {
        let mut jobs = self.jobs.write();
        let mut all = self.all_tasks.write();
        for tid in task_ids {
            let Some(job) = jobs.values_mut().find(|j| j.tasks.contains_key(tid)) else {
                continue;
            };
            let Some(info) = job.tasks.get_mut(tid) else {
                continue;
            };
            if info.state == JobState::Running && info.worker_id != worker_id {
                // Confirmed running elsewhere: not ours to take. Stealing
                // must go through eviction (owner death) first.
                continue;
            }
            info.state = JobState::Deploying;
            info.worker_id = worker_id.to_string();
            if job.state == JobState::Scheduled || job.state == JobState::Created {
                job.state = JobState::Deploying;
            }
            // The durable reassignment point (failover moves ownership).
            if let Some(desc) = all.get_mut(tid) {
                desc.config
                    .insert("worker_id".to_string(), worker_id.to_string());
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
        self.report_task_status_from("", job_id, task_id, state, records, error);
    }

    /// `report_task_status` with the reporting worker: a RUNNING report
    /// from a worker that does not own the task never steals ownership —
    /// the duplicate runner is fenced instead (systematic protection
    /// against double dispatch, not just on re-registration).
    pub fn report_task_status_from(
        &self,
        worker_id: &str,
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
        let new_state = JobState::from(state);
        if let Some(task) = job.tasks.get_mut(task_id) {
            if new_state == JobState::Running
                && task.state == JobState::Running
                && !worker_id.is_empty()
                && task.worker_id != worker_id
            {
                // Another worker already runs this task: keep the owner,
                // fence the duplicate runner.
                warn!(
                    "Task {} reported RUNNING by {} but owned by {}; fencing duplicate",
                    task_id, worker_id, task.worker_id
                );
                let owner = task.worker_id.clone();
                drop(jobs);
                self.queue_preemption(worker_id, task_id);
                let _ = owner;
                return;
            }
            task.state = new_state;
            task.processed_records = records.max(task.processed_records);
            task.error = error;
            if !worker_id.is_empty() {
                task.worker_id = worker_id.to_string();
            }
        } else {
            warn!(
                "status report for unknown task {} in job {}",
                task_id, job_id
            );
            return;
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

    /// Ingest live per-task metrics carried by worker heartbeats: record
    /// counter, last-record timestamp and the log-line increment. Task ids
    /// are globally unique, so the owning job is located by scanning.
    pub fn report_task_metrics(
        &self,
        task_id: &str,
        records: u64,
        last_record_at: i64,
        logs: Vec<String>,
        worker_id: &str,
        last_checkpoint_id: u64,
        last_checkpoint_size: u64,
    ) {
        let mut jobs = self.jobs.write();
        for job in jobs.values_mut() {
            if let Some(task) = job.tasks.get_mut(task_id) {
                task.processed_records = records.max(task.processed_records);
                if !worker_id.is_empty() {
                    task.worker_id = worker_id.to_string();
                }
                if last_record_at > 0 {
                    task.last_record_at = last_record_at;
                }
                if last_checkpoint_id > task.last_checkpoint_id {
                    task.last_checkpoint_id = last_checkpoint_id;
                    task.last_checkpoint_size = last_checkpoint_size;
                }
                if !job.state.is_terminal() && !logs.is_empty() {
                    task.append_logs(logs);
                }
                return;
            }
        }
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

    /// Decide and fire coordinated checkpoint triggers due on this
    /// heartbeat, for tasks owned by `worker_id`.
    ///
    /// The master is the checkpoint driver (Java CheckpointCoordinator
    /// role): a pipeline (stage) with no checkpoint in flight and whose
    /// interval elapsed gets one master-assigned id; every Running task
    /// of the stage participates. Ids come from the job's exported
    /// counter, so they never rewind across failover.
    /// Read-only due detection: pipelines of Running jobs whose interval
    /// elapsed and that have no checkpoint in flight. The handler
    /// proposes `Command::CheckpointTriggered` per stage (Direct mode:
    /// applies it in place), then delivers via
    /// [`Self::deliver_checkpoint_triggers`].
    pub fn due_checkpoint_stages(&self) -> Vec<(String, String)> {
        let now = seatunnel_engine_core::now_millis();
        let jobs = self.jobs.read();
        let pending = self.pending_checkpoints.read();
        let mut due = Vec::new();
        for job in jobs.values() {
            if job.state != JobState::Running {
                continue;
            }
            if job.last_checkpoint_trigger_ms > 0
                && now - job.last_checkpoint_trigger_ms < job.checkpoint_interval_ms as i64
            {
                continue;
            }
            let mut stages: std::collections::HashSet<String> = std::collections::HashSet::new();
            for info in job.tasks.values() {
                if info.state == JobState::Running {
                    stages.insert(info.stage_id.clone());
                }
            }
            for stage_id in stages {
                if !pending.contains_key(&(job.job_id.clone(), stage_id.clone())) {
                    due.push((job.job_id.clone(), stage_id));
                }
            }
        }
        due
    }

    /// Direct-mode convenience: trigger every due stage in place, then
    /// deliver to `worker_id` (the pre-Raft single-call behavior).
    pub fn due_checkpoint_triggers(
        &self,
        worker_id: &str,
    ) -> Vec<seatunnel_engine_comm::CheckpointTrigger> {
        let now = seatunnel_engine_core::now_millis();
        for (job_id, stage_id) in self.due_checkpoint_stages() {
            self.apply_command(&Command::CheckpointTriggered {
                job_id,
                stage_id,
                at_ms: now,
            });
        }
        self.deliver_checkpoint_triggers(worker_id)
    }

    /// Deliver undelivered triggers of pending checkpoints to this
    /// worker's heartbeat — survives the interval gate (a worker that
    /// heartbeats late still gets its trigger).
    pub fn deliver_checkpoint_triggers(
        &self,
        worker_id: &str,
    ) -> Vec<seatunnel_engine_comm::CheckpointTrigger> {
        let mut triggers = Vec::new();
        let mut pending = self.pending_checkpoints.write();
        for cp in pending.values_mut() {
            for (task_id, owner) in &cp.participants {
                if owner == worker_id && !cp.delivered.contains(task_id) {
                    cp.delivered.insert(task_id.clone());
                    triggers.push(seatunnel_engine_comm::CheckpointTrigger {
                        task_id: task_id.clone(),
                        checkpoint_id: cp.checkpoint_id,
                    });
                }
            }
        }
        triggers
    }

    /// Direct-mode wrapper: decide + apply the resolution in place.
    pub fn handle_checkpoint_prepare(
        &self,
        job_id: &str,
        task_id: &str,
        checkpoint_id: u64,
        success: bool,
    ) -> Option<u64> {
        let (stage_id, completed, participants) =
            self.note_checkpoint_prepare(job_id, task_id, checkpoint_id, success)?;
        self.apply_command(&Command::CheckpointResolved {
            job_id: job_id.to_string(),
            stage_id,
            checkpoint_id,
            completed,
            participants,
        });
        Some(checkpoint_id)
    }

    /// Read-only decision half: ingest one task's prepare and decide the
    /// resolution. Consensus mode's handler proposes the returned
    /// resolution as `Command::CheckpointResolved`.
    pub fn note_checkpoint_prepare(
        &self,
        job_id: &str,
        task_id: &str,
        checkpoint_id: u64,
        success: bool,
    ) -> Option<(String, bool, Vec<(String, String)>)> {
        let stage_id = {
            let jobs = self.jobs.read();
            jobs.get(job_id)?
                .tasks
                .get(task_id)?
                .stage_id
                .clone()
        };
        let key = (job_id.to_string(), stage_id.clone());
        // Awaiting-bookkeeping is leader-local (a leader switch drops
        // pendings); the durable resolution is a Command. Decide only:
        let mut decision: Option<(bool, Vec<(String, String)>)> = None;
        {
            let mut pending = self.pending_checkpoints.write();
            let Some(cp) = pending.get_mut(&key) else {
                tracing::debug!(
                    "checkpoint prepare for {} but no pending checkpoint (job {})",
                    checkpoint_id,
                    job_id
                );
                return None;
            };
            if cp.checkpoint_id != checkpoint_id {
                tracing::warn!(
                    "checkpoint prepare for {} but pending is {}; ignoring",
                    checkpoint_id,
                    cp.checkpoint_id
                );
                return None;
            }
            if !success {
                let participants: Vec<(String, String)> = cp
                    .participants
                    .iter()
                    .map(|(t, w)| (t.clone(), w.clone()))
                    .collect();
                decision = Some((false, participants));
            } else {
                cp.awaiting.retain(|id| id != task_id);
                if cp.awaiting.is_empty() {
                    let participants: Vec<(String, String)> = cp
                    .participants
                    .iter()
                    .map(|(t, w)| (t.clone(), w.clone()))
                    .collect();
                    decision = Some((true, participants));
                }
            }
        }
        let (completed, participants) = decision?;
        if completed {
            info!(
                "Job {} pipeline {} checkpoint {} complete: {} task(s) prepared",
                job_id, stage_id, checkpoint_id, participants.len()
            );
        } else {
            warn!(
                "Job {} pipeline {} checkpoint {} failed at task {}; aborting",
                job_id, stage_id, checkpoint_id, task_id
            );
        }
        Some((stage_id, completed, participants))
    }

    /// Resolve a pending checkpoint: queue per-task events (complete →
    /// workers run 2PC phase 2 / abort → unwind) for delivery on the
    /// owning workers' next heartbeats.
    fn resolve(&self, cp: &PendingCheckpoint, completed: bool) {
        let mut outbox = self.checkpoint_outbox.lock().unwrap();
        for (task_id, worker_id) in &cp.participants {
            outbox
                .entry(worker_id.clone())
                .or_default()
                .push(seatunnel_engine_comm::CheckpointResolution {
                    task_id: task_id.clone(),
                    checkpoint_id: cp.checkpoint_id,
                    completed,
                });
        }
    }

    /// Take the resolution events queued for a worker's heartbeat.
    pub fn drain_checkpoint_resolutions(
        &self,
        worker_id: &str,
    ) -> Vec<seatunnel_engine_comm::CheckpointResolution> {
        self.checkpoint_outbox
            .lock()
            .unwrap()
            .remove(worker_id)
            .unwrap_or_default()
    }

    /// Abort coordinated checkpoints whose prepares did not arrive in
    /// time (a stuck or dead participant). Returns how many were aborted.
    pub fn abort_timed_out_checkpoints(&self, timeout_ms: u64) -> usize {
        let now = seatunnel_engine_core::now_millis();
        let mut pending = self.pending_checkpoints.write();
        let expired: Vec<(String, String)> = pending
            .iter()
            .filter(|(_, cp)| now - cp.triggered_at_ms > timeout_ms as i64)
            .map(|(k, _)| k.clone())
            .collect();
        let count = expired.len();
        for key in expired {
            if let Some(cp) = pending.remove(&key) {
                warn!(
                    "Job {} pipeline {} checkpoint {} timed out ({} task(s) unreported); aborting",
                    key.0,
                    cp.stage_id,
                    cp.checkpoint_id,
                    cp.awaiting.len()
                );
                self.resolve(&cp, false);
            }
        }
        count
    }

    /// Drop in-flight checkpoints of a job (cancel/terminal cleanup).
    pub fn drop_pending_checkpoints(&self, job_id: &str) {
        let mut pending = self.pending_checkpoints.write();
        let keys: Vec<(String, String)> = pending
            .keys()
            .filter(|(j, _)| j == job_id)
            .cloned()
            .collect();
        for key in keys {
            if let Some(cp) = pending.remove(&key) {
                self.resolve(&cp, false);
            }
        }
    }

    /// Cancel a job. Returns true if a running/scheduled job was cancelled.
    pub fn cancel_job(&self, job_id: &str) -> bool {
        let cancelled = {
            let mut jobs = self.jobs.write();
            if let Some(job) = jobs.get_mut(job_id) {
                if !job.state.is_terminal() {
                    job.state = JobState::Cancelled;
                    job.end_time = Some(seatunnel_engine_core::now_millis());
                    true
                } else {
                    false
                }
            } else {
                false
            }
        };
        if cancelled {
            info!("Job {} cancelled by user", job_id);
            // Abort any in-flight coordinated checkpoint (workers unwind).
            self.drop_pending_checkpoints(job_id);
        }
        cancelled
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
            .compile_and_install("j1", "j", &config, None, &workers(2))
            .unwrap();
        // Dispatch both tasks to their assigned workers.
        let ids: Vec<String> = tasks.iter().map(|t| t.task_id.clone()).collect();
        coordinator.mark_tasks_dispatched(&ids, "worker-0");
        coordinator.mark_tasks_dispatched(&ids[1..], "worker-1");

        // worker-1 dies: its task becomes CLAIMABLE by worker-0 — the
        // read-only claim returns it (still labeled with the dead owner);
        // the durable reassignment happens in mark_tasks_dispatched.
        let dead = "worker-1";
        let claimed =
            coordinator.claim_tasks_for_worker("worker-0", "127.0.0.1:9999", &|w| w != dead);
        let orphan = claimed.iter().find(|t| t.task_id == ids[1]).expect("orphan claimed");
        assert_eq!(
            orphan.config.get("worker_id").map(String::as_str),
            Some("worker-1"),
            "claim is read-only; reassignment lands with the dispatch command"
        );

        coordinator.mark_tasks_dispatched(std::slice::from_ref(&ids[1]), "worker-0");
        // The descriptor (assignment source of truth) moved to worker-0,
        // and a live peer can no longer claim it.
        let snapshot = tokio::runtime::Runtime::new().unwrap().block_on(coordinator.export_state());
        let reassigned = snapshot["tasks"]
            .as_array()
            .unwrap()
            .iter()
            .find(|t| t["task_id"] == ids[1].as_str())
            .unwrap();
        assert_eq!(reassigned["config"]["worker_id"], "worker-0");
        let stolen = coordinator.claim_tasks_for_worker("worker-2", "a", &|w| w != dead);
        assert!(stolen.iter().all(|t| t.task_id != ids[1]));
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
            .compile_and_install("j2", "j", &config, None, &workers(1))
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
            .compile_and_install("j3", "j", &config, None, &workers(1))
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
        assert!(
            coordinator
                .register_running_tasks("worker-1", &[task_id])
                .is_empty()
        );
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
            .compile_and_install("jx", "j", &config, None, &workers(1))
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
    fn test_term_never_regresses() {
        let coordinator = JobCoordinator::new();
        assert_eq!(coordinator.term(), 1);
        assert_eq!(coordinator.observe_term(5), 5);
        assert_eq!(coordinator.term(), 5);
        // A lower observed term (stale master snapshot) is ignored.
        assert_eq!(coordinator.observe_term(3), 5);
        assert_eq!(coordinator.term(), 5);
    }

    #[tokio::test]
    async fn test_term_travels_with_state_snapshot() {
        let primary = JobCoordinator::new();
        primary.observe_term(9);
        let snapshot = primary.export_state().await;
        assert_eq!(snapshot["term"], 9);

        let standby = JobCoordinator::new();
        standby.import_state(&snapshot).await;
        assert_eq!(standby.term(), 9);
    }

    #[test]
    fn test_register_running_tasks_adopts_own_tasks() {
        let coordinator = JobCoordinator::new();
        let config = json!({
            "env": { "parallelism": 1 },
            "source": { "Fake": {} },
            "sink": { "Console": {} }
        });
        let (_, tasks) = coordinator
            .compile_and_install("ja", "a", &config, None, &workers(1))
            .unwrap();
        let task_id = tasks[0].task_id.clone();
        coordinator.mark_tasks_dispatched(std::slice::from_ref(&task_id), "worker-0");

        // Reconnect of the owning worker: adopt, never preempt.
        let preempted =
            coordinator.register_running_tasks("worker-0", std::slice::from_ref(&task_id));
        assert!(preempted.is_empty());
        // The task is Running for worker-0 and NOT claimable by a live peer.
        let job = coordinator.get_job("ja").unwrap();
        assert_eq!(job.tasks[&task_id].state, JobState::Running);
        let claimed = coordinator.claim_tasks_for_worker("worker-1", "a", &|_| true);
        assert!(claimed.iter().all(|t| t.task_id != task_id));
    }

    #[test]
    fn test_coordinated_checkpoint_lifecycle() {
        let coordinator = JobCoordinator::new();
        let config = json!({
            "env": { "parallelism": 2, "checkpoint": { "interval": 300000 } },
            "source": { "Fake": {} },
            "sink": { "Console": {} }
        });
        let (job_id, tasks) = coordinator
            .compile_and_install("jcc", "cc", &config, None, &workers(2))
            .unwrap();
        // Both tasks Running on their assigned workers.
        for (i, t) in tasks.iter().enumerate() {
            let owner = format!("worker-{}", i);
            coordinator.mark_tasks_dispatched(std::slice::from_ref(&t.task_id), &owner);
            coordinator.report_task_status(&job_id, &t.task_id, "RUNNING", 0, None);
        }
        let t0 = &tasks[0].task_id;
        let t1 = &tasks[1].task_id;
        // Heartbeat of worker-0 triggers the pipeline: it sees its own
        // task's trigger; worker-1 gets its own on its heartbeat, with
        // the SAME coordinated checkpoint id.
        let tr0 = coordinator.due_checkpoint_triggers("worker-0");
        let tr1 = coordinator.due_checkpoint_triggers("worker-1");
        assert_eq!(tr0.len(), 1);
        assert_eq!(tr1.len(), 1);
        assert_eq!(tr0[0].task_id, *t0);
        assert_eq!(tr1[0].task_id, *t1);
        let cp_id = tr0[0].checkpoint_id;
        assert_eq!(tr1[0].checkpoint_id, cp_id);

        // Interval gating: no further triggers until the interval elapses.
        assert!(coordinator.due_checkpoint_triggers("worker-0").is_empty());

        // First prepare: not resolved yet, no resolutions delivered.
        assert_eq!(
            coordinator.handle_checkpoint_prepare(&job_id, t0, cp_id, true),
            None
        );
        assert!(coordinator.drain_checkpoint_resolutions("worker-0").is_empty());
        assert!(coordinator.drain_checkpoint_resolutions("worker-1").is_empty());

        // Second prepare resolves: completed events for both workers.
        assert_eq!(
            coordinator.handle_checkpoint_prepare(&job_id, t1, cp_id, true),
            Some(cp_id)
        );
        for w in ["worker-0", "worker-1"] {
            let rs = coordinator.drain_checkpoint_resolutions(w);
            assert_eq!(rs.len(), 1);
            assert!(rs[0].completed);
            assert_eq!(rs[0].checkpoint_id, cp_id);
        }
        let job = coordinator.get_job(&job_id).unwrap();
        assert_eq!(job.checkpoints_completed, 1);
        // Id counter advanced past the resolved checkpoint.
        assert!(job.next_checkpoint_id > cp_id);
    }

    #[test]
    fn test_coordinated_checkpoint_failure_and_timeout_abort() {
        // Failure path: one task's barrier fails → abort for everyone.
        let coordinator = JobCoordinator::new();
        let config = json!({
            "env": { "parallelism": 2, "checkpoint": { "interval": 300000 } },
            "source": { "Fake": {} },
            "sink": { "Console": {} }
        });
        let (job_id, tasks) = coordinator
            .compile_and_install("jcf", "f", &config, None, &workers(2))
            .unwrap();
        for (i, t) in tasks.iter().enumerate() {
            coordinator.report_task_status(
                &job_id,
                &t.task_id,
                "RUNNING",
                0,
                None,
            );
            let _ = i;
        }
        let tr = coordinator.due_checkpoint_triggers("worker-0");
        assert_eq!(tr.len(), 1);
        let cp_id = tr[0].checkpoint_id;
        assert_eq!(
            coordinator.handle_checkpoint_prepare(&job_id, &tasks[0].task_id, cp_id, false),
            Some(cp_id)
        );
        for w in ["worker-0", "worker-1"] {
            let rs = coordinator.drain_checkpoint_resolutions(w);
            assert_eq!(rs.len(), 1);
            assert!(!rs[0].completed, "failure must abort, not complete");
        }

        // Timeout path: prepares never arrive → sweep aborts.
        {
            // Rewind the volatile interval gate so the next round fires.
            let mut jobs = coordinator.jobs.write();
            jobs.get_mut(&job_id).unwrap().last_checkpoint_trigger_ms = 0;
        }
        let tr2 = coordinator.due_checkpoint_triggers("worker-0");
        assert_eq!(tr2.len(), 1);
        // The strict deadline comparison needs the trigger to age at
        // least one millisecond (production timeouts are 30s+).
        std::thread::sleep(std::time::Duration::from_millis(3));
        let aborted = coordinator.abort_timed_out_checkpoints(0);
        assert_eq!(aborted, 1);
        let rs = coordinator.drain_checkpoint_resolutions("worker-0");
        assert_eq!(rs.len(), 1);
        assert!(!rs[0].completed);
    }

    #[tokio::test]
    async fn test_checkpoint_ids_never_rewind_across_ha_snapshot() {
        let primary = JobCoordinator::new();
        let config = json!({
            "env": { "parallelism": 1, "checkpoint": { "interval": 300000 } },
            "source": { "Fake": {} },
            "sink": { "Console": {} }
        });
        let (job_id, tasks) = primary
            .compile_and_install("jha", "ha", &config, None, &workers(1))
            .unwrap();
        primary.report_task_status(&job_id, &tasks[0].task_id, "RUNNING", 0, None);
        // Consume two coordinated checkpoint ids on the primary. The
        // interval gate must be rewound between rounds (volatile field).
        for _ in 0..2 {
            primary.jobs.write().get_mut(&job_id).unwrap().last_checkpoint_trigger_ms = 0;
            let tr = primary.due_checkpoint_triggers("worker-0");
            assert_eq!(tr.len(), 1);
            let cp = tr[0].checkpoint_id;
            primary.handle_checkpoint_prepare(&job_id, &tasks[0].task_id, cp, true);
        }
        let snapshot = primary.export_state().await;

        // A standby that already holds the job with a LOWER counter must
        // never rewind below the primary's allocation.
        let standby = JobCoordinator::new();
        standby
            .compile_and_install("jha", "ha", &config, None, &workers(1))
            .unwrap();
        standby.import_state(&snapshot).await;
        let imported_next = standby.get_job("jha").unwrap().next_checkpoint_id;
        let primary_next = primary.get_job("jha").unwrap().next_checkpoint_id;
        assert!(imported_next >= primary_next);
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
            .compile_and_install("j2", "multi", &config, None, &workers(2))
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
        assert_eq!(
            t2.config.get("pipeline.on-sink-failure").unwrap(),
            "isolate"
        );
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
            .compile_and_install("j3", "legacy", &config, None, &workers(1))
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
            .compile_and_install("j1", "cdc-job", &config, None, &workers(3))
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
        assert!(
            coordinator
                .get_pending_tasks_for_worker("worker-0")
                .is_empty()
        );
    }

    #[test]
    fn test_no_workers_is_error() {
        let coordinator = JobCoordinator::new();
        let config = json!({
            "source": { "Fake": { "row.num": 10 } },
            "sink": { "Console": {} }
        });
        let result = coordinator.compile_and_install("jx", "x", &config, None, &[]);
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
            .compile_and_install("jr", "r", &config, Some(2), &workers(1))
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
            .compile_and_install("jf", "f", &config, Some(1), &workers(1))
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
            .compile_and_install("jc", "c", &config, Some(1), &workers(1))
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
