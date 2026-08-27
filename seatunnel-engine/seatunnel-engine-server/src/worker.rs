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

//! Worker task executor.
//!
//! Receives chained TaskDescriptors from the master via heartbeat, builds the
//! real Source → Transform → Sink chain through the shared connector factory,
//! runs it in a `TaskGroup` with checkpointing enabled, reports lifecycle
//! transitions back to the master and persists checkpoint state locally so a
//! restarted worker resumes from the last binlog position / offset.

use std::collections::HashMap;
use std::sync::Arc;

use tokio::sync::Mutex;
use tokio_util::sync::CancellationToken;
use tracing::{error, info, warn};

use seatunnel_engine_comm::{
    generated::master_service_client::MasterServiceClient, CheckpointReport, TaskDescriptor,
    TaskStatusReport,
};
use seatunnel_engine_core::checkpoint_listener::CheckpointListener;
use seatunnel_engine_core::connector_factory::{create_sink, create_sinks, create_source, create_transforms};
use seatunnel_engine_core::state::TaskState;
use seatunnel_engine_core::task_group::TaskGroup;
use seatunnel_engine_core::{now_millis, TaskStatus};

use crate::state_store::LocalStateStore;

/// Information about a task executing on this worker.
#[derive(Debug)]
pub struct RunningTask {
    pub task_id: String,
    pub job_id: String,
    pub stage_id: String,
    pub state: String,
    pub processed_records: u64,
    pub start_time: i64,
}

type SharedMasterClient = Arc<Mutex<Option<MasterServiceClient<tonic::transport::Channel>>>>;

/// Worker node that executes chained pipeline tasks assigned by the master.
pub struct WorkerNode {
    /// Auto-cleanup settings (0/None disables the cleaner).
    clean_config: Option<CleanConfig>,
    /// Checkpoint storage backend: localfile | master | s3.
    storage_type: String,
    /// S3 store when storage_type = s3.
    s3_store: Option<crate::checkpoint_store::S3CheckpointStore>,
    worker_id: String,
    #[allow(dead_code)] // reported to the master once registration is wired up
    address: String,
    master_client: SharedMasterClient,
    state_store: Arc<LocalStateStore>,
    running_tasks: Mutex<HashMap<String, RunningTaskHandle>>,
}

/// Internal handle for a spawned task execution.
#[derive(Debug)]
struct RunningTaskHandle {
    job_id: String,
    cancel: Arc<CancellationToken>,
}

impl WorkerNode {
    pub fn new(
        worker_id: impl Into<String>,
        address: impl Into<String>,
        state_store: Arc<LocalStateStore>,
    ) -> Self {
        Self::new_with_clean(worker_id, address, state_store, None)
    }

    /// Construct with auto-cleanup enabled (engine `seatunnel.yaml`).
    pub fn new_with_clean(
        worker_id: impl Into<String>,
        address: impl Into<String>,
        state_store: Arc<LocalStateStore>,
        clean_config: Option<CleanConfig>,
    ) -> Self {
        WorkerNode {
            worker_id: worker_id.into(),
            address: address.into(),
            master_client: Arc::new(Mutex::new(None)),
            state_store,
            running_tasks: Mutex::new(HashMap::new()),
            clean_config,
            storage_type: "localfile".to_string(),
            s3_store: None,
        }
    }

    /// Attach the checkpoint storage backend (master/s3) for failover.
    pub fn with_checkpoint_storage(
        &mut self,
        storage_type: &str,
        s3_store: Option<crate::checkpoint_store::S3CheckpointStore>,
    ) {
        self.storage_type = storage_type.to_string();
        self.s3_store = s3_store;
    }

    /// Set the gRPC client used for reporting to the master.
    pub async fn set_master_client(&self, client: MasterServiceClient<tonic::transport::Channel>) {
        *self.master_client.lock().await = Some(client);
    }

    pub fn state_store(&self) -> &Arc<LocalStateStore> {
        &self.state_store
    }

    /// Accept a task from the master and start executing it asynchronously.
    /// Completion removes the task from the local registry.
    pub async fn assign_task(self: &Arc<Self>, task: TaskDescriptor) {
        let task_id = task.task_id.clone();
        let job_id = task.job_id.clone();

        // Failover dedup: if this worker already runs the task (e.g. the
        // master re-dispatched during a reconnect window), skip it.
        if self.running_tasks.lock().await.contains_key(&task_id) {
            info!(
                "Worker {}: task {} already running locally — dispatch ignored",
                self.worker_id, task_id
            );
            return;
        }

        info!(
            "Worker {}: accepting task {} (subtask {}/{})",
            self.worker_id, task.task_id, task.task_index, task.parallelism
        );

        let cancel = Arc::new(CancellationToken::new());
        self.running_tasks.lock().await.insert(
            task_id.clone(),
            RunningTaskHandle {
                job_id: job_id.clone(),
                cancel: cancel.clone(),
            },
        );

        let ctx = TaskExecCtx {
            worker_id: self.worker_id.clone(),
            master_client: self.master_client.clone(),
            state_store: self.state_store.clone(),
            storage_type: self.storage_type.clone(),
            s3_store: self.s3_store.clone(),
        };

        let worker = Arc::clone(self);
        let cleanup_task_id = task_id.clone();
        tokio::spawn(async move {
            execute_descriptor(task, ctx, cancel).await;
            // Detach from the registry once terminal.
            worker.running_tasks.lock().await.remove(&cleanup_task_id);
        });
    }

    /// Ids of tasks currently running on this worker (fencing reports).
    pub async fn running_task_ids(&self) -> Vec<String> {
        self.running_tasks.lock().await.keys().cloned().collect()
    }

    /// Cancel specific tasks that were reassigned elsewhere by the master
    /// (failover fencing) so they are not executed twice.
    pub async fn preempt_tasks(&self, task_ids: &[String]) {
        if task_ids.is_empty() {
            return;
        }
        let mut tasks = self.running_tasks.lock().await;
        for task_id in task_ids {
            if let Some(handle) = tasks.get_mut(task_id) {
                warn!(
                    "Worker {}: preempting task {} (reassigned by the master)",
                    self.worker_id, task_id
                );
                handle.cancel.cancel();
            }
        }
    }

    /// Snapshot of running tasks for the next heartbeat.
    pub async fn heartbeat_tasks(&self) -> Vec<seatunnel_engine_comm::TaskHeartbeat> {
        self.running_tasks
            .lock()
            .await
            .keys()
            .map(|tid| seatunnel_engine_comm::TaskHeartbeat {
                task_id: tid.clone(),
                state: 2, // TASK_RUNNING
                processed_records: 0,
                last_heartbeat_time: now_millis(),
                memory_usage: 0,
            })
            .collect()
    }

    /// Stop all local tasks belonging to the given jobs.
    pub async fn cancel_jobs(&self, job_ids: &[String]) {
        if job_ids.is_empty() {
            return;
        }
        // Auto-clean: drop the cancelled jobs' local state after the
        // configured grace window.
        if let Some(clean) = &self.clean_config {
            for job_id in job_ids {
                schedule_cancel_cleanup(
                    Arc::clone(&self.state_store),
                    job_id.clone(),
                    clean.grace_secs,
                );
            }
        }
        let mut tasks = self.running_tasks.lock().await;
        for (tid, handle) in tasks.iter_mut() {
            if job_ids.contains(&handle.job_id) && !handle.cancel.is_cancelled() {
                info!("Cancelling task {} (job cancelled)", tid);
                handle.cancel.cancel();
            }
        }
    }

    /// Count of currently tracked tasks.
    pub async fn running_task_count(&self) -> usize {
        self.running_tasks.lock().await.len()
    }
}

/// Report a task lifecycle transition to the master. Never consumes the
/// master client — transient RPC failures must not break later reports.
async fn report_transition_raw(
    worker_id: &str,
    master_client: &SharedMasterClient,
    job_id: &str,
    task_id: &str,
    state: i32,
    records: u64,
    error: Option<String>,
) {
    let mut guard = master_client.lock().await;
    let Some(client) = guard.as_mut() else {
        warn!(
            "no master client; cannot report state {} for task {} (worker {})",
            state, task_id, worker_id
        );
        return;
    };
    let report = TaskStatusReport {
        worker_id: worker_id.to_string(),
        task_id: task_id.to_string(),
        job_id: job_id.to_string(),
        state,
        timestamp: now_millis(),
        processed_records: records as i64,
        error_message: error.unwrap_or_default(),
    };
    if let Err(e) = client.report_task_status(tonic::Request::new(report)).await {
        warn!("report_task_status failed for {}: {}", task_id, e);
    }
}

/// Background state-cleanup settings derived from the engine config.
#[derive(Debug, Clone, Copy)]
pub struct CleanConfig {
    /// Seconds after a cancelled job's state is deleted.
    pub grace_secs: u64,
    /// Seconds between TTL sweeps.
    pub interval_secs: u64,
    /// Sweep TTL in seconds (history-job-expire-minutes).
    pub ttl_secs: u64,
}

/// Spawn the background cleaner: periodic TTL sweep plus delayed cleanup
/// of cancelled jobs (after the grace window).
pub fn spawn_state_cleaner(worker: Arc<WorkerNode>, config: CleanConfig) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let mut ticker = tokio::time::interval(std::time::Duration::from_secs(
            config.interval_secs.max(1),
        ));
        // First tick fires immediately — sweep once at startup.
        loop {
            ticker.tick().await;
            let removed = worker
                .state_store()
                .sweep_expired(std::time::Duration::from_secs(config.ttl_secs.max(1)));
            if !removed.is_empty() {
                tracing::info!("state cleaner: swept {} expired job(s)", removed.len());
            }
        }
    })
}

/// Fetch the newest checkpoint from the master-backed shared store.
async fn fetch_checkpoint_from_master(
    master_client: &SharedMasterClient,
    job_id: &str,
    task_id: &str,
) -> Option<(u64, Vec<u8>)> {
    let mut guard = master_client.lock().await;
    let client = guard.as_mut()?;
    let request = tonic::Request::new(seatunnel_engine_comm::FetchCheckpointRequest {
        job_id: job_id.to_string(),
        task_id: task_id.to_string(),
    });
    match client.fetch_checkpoint(request).await {
        Ok(resp) => {
            let inner = resp.into_inner();
            if inner.checkpoint_id > 0 && !inner.checkpoint_data.is_empty() {
                Some((inner.checkpoint_id as u64, inner.checkpoint_data))
            } else {
                None
            }
        }
        Err(e) => {
            warn!("fetch_checkpoint for {} failed: {}", task_id, e);
            None
        }
    }
}

/// Schedule deletion of a cancelled job's local state after the grace
/// window (keeps a restore window for operator intervention).
pub fn schedule_cancel_cleanup(
    state_store: Arc<LocalStateStore>,
    job_id: String,
    grace_secs: u64,
) {
    tokio::spawn(async move {
        tokio::time::sleep(std::time::Duration::from_secs(grace_secs.max(1))).await;
        state_store.drop_job(&job_id);
        tracing::info!("state cleaner: removed cancelled job state '{}'", job_id);
    });
}

/// Execution context handed to the spawned task future so it can report
/// transitions and reach the state store without borrowing the worker.
#[derive(Clone)]
struct TaskExecCtx {
    worker_id: String,
    master_client: SharedMasterClient,
    state_store: Arc<LocalStateStore>,
    /// Checkpoint storage backend: localfile | master | s3.
    storage_type: String,
    /// S3 store (storage type = s3); workers write directly.
    s3_store: Option<crate::checkpoint_store::S3CheckpointStore>,
}

/// Execute one descriptor end-to-end: build connectors, restore state, run
/// the TaskGroup, and report every transition to the master.
///
/// Panics inside the pipeline are caught and converted into a FAILED report
/// so a buggy connector cannot take down the whole worker process silently.
async fn execute_descriptor(
    task: TaskDescriptor,
    ctx: TaskExecCtx,
    cancel: Arc<CancellationToken>,
) {
    let task_id = task.task_id.clone();
    let job_id = task.job_id.clone();

    report_transition_raw(
        &ctx.worker_id,
        &ctx.master_client,
        &job_id,
        &task_id,
        2,
        0,
        None,
    )
    .await;

    let result = match futures::FutureExt::catch_unwind(std::panic::AssertUnwindSafe(run_pipeline(
        &task, &ctx, cancel,
    )))
    .await
    {
        Ok(Ok(status)) => Ok(status),
        Ok(Err(e)) => Err(e),
        Err(panic_payload) => {
            let msg = panic_message(&panic_payload);
            Err(anyhow::anyhow!("task panicked: {}", msg))
        }
    };

    match result {
        Ok(status) => {
            let (code, err) = match status.state {
                TaskState::Completed => (3, None),
                TaskState::Cancelled => (5, None),
                TaskState::Failed { ref error } => (4, Some(error.clone())),
                other => (2, Some(format!("unexpected state {}", other))),
            };
            report_transition_raw(
                &ctx.worker_id,
                &ctx.master_client,
                &job_id,
                &task_id,
                code,
                status.processed_records,
                err,
            )
            .await;
        }
        Err(e) => {
            error!("Task {} crashed: {}", task_id, e);
            report_transition_raw(
                &ctx.worker_id,
                &ctx.master_client,
                &job_id,
                &task_id,
                4,
                0,
                Some(e.to_string()),
            )
            .await;
        }
    }
}

fn panic_message(payload: &Box<dyn std::any::Any + Send>) -> String {
    if let Some(s) = payload.downcast_ref::<&str>() {
        (*s).to_string()
    } else if let Some(s) = payload.downcast_ref::<String>() {
        s.clone()
    } else {
        "unknown panic".to_string()
    }
}

/// Build and run the chained pipeline for a descriptor.
async fn run_pipeline(
    task: &TaskDescriptor,
    ctx: &TaskExecCtx,
    cancel: Arc<CancellationToken>,
) -> anyhow::Result<TaskStatus> {
    let cfg = &task.config;

    // Multi-pipeline jobs carry `pipeline.*` keys (source + sink list,
    // fan-out built inside the task); legacy single-pipeline descriptors
    // keep source.plugin/sink.plugin.
    let source_plugin = cfg
        .get("pipeline.source.plugin")
        .or_else(|| cfg.get("source.plugin"))
        .map(String::as_str)
        .unwrap_or("");
    let source_config_raw = cfg
        .get("pipeline.source.config")
        .or_else(|| cfg.get("source.config"))
        .map(String::as_str)
        .unwrap_or("{}");
    let transform_raw = cfg
        .get("transform.config")
        .map(String::as_str)
        .unwrap_or("[]");
    let checkpoint_interval: u64 = cfg
        .get("checkpoint.interval")
        .and_then(|v| v.parse().ok())
        .unwrap_or(crate::job_coordinator::DEFAULT_CHECKPOINT_INTERVAL_MS);

    let source_value: serde_json::Value = serde_json::from_str(source_config_raw)?;
    let mut source_config =
        seatunnel_engine_core::connector_factory::json_to_config_map(&source_value);
    // Partition snapshot ranges across parallel subtasks (within this
    // pipeline — task_index/parallelism are pipeline-scoped).
    source_config.insert("subtask.index".to_string(), task.task_index.to_string());
    source_config.insert(
        "subtask.count".to_string(),
        task.parallelism.max(1).to_string(),
    );
    let transforms_cfg: Vec<serde_json::Value> = serde_json::from_str(transform_raw)?;

    // Restore chain: worker-local disk > shared backend (master store /
    // S3 by storage type) > cold start. A task taken over from a dead
    // worker resumes from the shared checkpoint instead of re-snapshotting.
    let restore_state = if let Some((id, data)) = ctx
        .state_store
        .load_latest_checkpoint(&task.job_id, &task.task_id)
        .ok()
        .flatten()
    {
        info!(
            "Task {}: restored checkpoint cp-{} from local",
            task.task_id, id
        );
        Some(data)
    } else {
        match ctx.storage_type.as_str() {
            "master" => fetch_checkpoint_from_master(
                &ctx.master_client,
                &task.job_id,
                &task.task_id,
            )
            .await
            .map(|(id, data)| {
                info!(
                    "Task {}: restored checkpoint cp-{} from master",
                    task.task_id, id
                );
                data
            }),
            "s3" => {
                let fetched = if let Some(store) = &ctx.s3_store {
                    store
                        .load_latest(&task.job_id, &task.task_id)
                        .await
                } else {
                    None
                };
                fetched.map(|(id, data)| {
                    info!(
                        "Task {}: restored checkpoint cp-{} from s3",
                        task.task_id, id
                    );
                    data
                })
            }
            _ => {
                info!(
                    "Task {}: no checkpoint found (cold start)",
                    task.task_id
                );
                None
            }
        }
    };

    let reader = create_source(
        source_plugin,
        &source_config,
        task.parallelism.max(1) as usize,
        restore_state.as_deref(),
    )?;
    let transforms = create_transforms(&transforms_cfg)?;

    // Sink side: pipeline descriptors carry a sink LIST (fan-out through
    // the FanoutSinkWriter mux); legacy descriptors fall back to a single
    // sink.plugin/sink.config pair.
    let writer = if let Some(sinks_raw) = cfg.get("pipeline.sinks") {
        let sinks: Vec<seatunnel_engine_core::connector_factory::SinkDeclaration> =
            serde_json::from_str(sinks_raw)?;
        let policy = seatunnel_engine_core::fanout::SinkFailurePolicy::parse(
            cfg.get("pipeline.on-sink-failure")
                .map(String::as_str)
                .unwrap_or("fail"),
        );
        info!(
            "Task {}: pipeline '{}' → {} sink(s), on-sink-failure={:?}",
            task.task_id,
            cfg.get("pipeline.name").map(String::as_str).unwrap_or("?"),
            sinks.len(),
            policy
        );
        create_sinks(&sinks, policy)?
    } else {
        let sink_plugin = cfg
            .get("sink.plugin")
            .map(String::as_str)
            .unwrap_or("console");
        let sink_value: serde_json::Value =
            serde_json::from_str(cfg.get("sink.config").map(String::as_str).unwrap_or("{}"))?;
        let sink_config =
            seatunnel_engine_core::connector_factory::json_to_config_map(&sink_value);
        create_sink(sink_plugin, &sink_config)?
    };

    let context = seatunnel_engine_core::task_group::TaskContext::new(
        task.task_id.clone(),
        task.job_id.clone(),
        task.stage_id.clone(),
        task.task_index.max(0) as usize,
        task.parallelism.max(1) as usize,
    )
    .with_cancel_token(cancel)
    .with_checkpoint_interval(checkpoint_interval)
    .with_checkpoint_listener(Arc::new(TaskCheckpointReporter {
        worker_id: ctx.worker_id.clone(),
        master_client: ctx.master_client.clone(),
        state_store: ctx.state_store.clone(),
        upload_to_master: ctx.storage_type == "master",
        s3_store: ctx.s3_store.clone(),
    }));

    let mut group = TaskGroup::new(context, reader, writer).with_transforms(transforms);
    group.run().await
}

/// Checkpoint listener that persists state durably and reports success to the
/// master over gRPC.
struct TaskCheckpointReporter {
    #[allow(dead_code)] // included in future checkpoint reports to the master
    worker_id: String,
    master_client: SharedMasterClient,
    state_store: Arc<LocalStateStore>,
    /// Upload bytes to the master-backed store (storage type = master).
    upload_to_master: bool,
    /// Direct S3 writes (storage type = s3).
    s3_store: Option<crate::checkpoint_store::S3CheckpointStore>,
}

impl CheckpointListener for TaskCheckpointReporter {
    fn on_checkpoint<'a>(
        &'a self,
        job_id: &'a str,
        task_id: &'a str,
        checkpoint_id: u64,
        timestamp: i64,
        state: Vec<u8>,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send + 'a>> {
        Box::pin(async move {
            // 1. Durable local persistence (crash recovery).
            if let Err(e) = self
                .state_store
                .save_checkpoint(job_id, task_id, checkpoint_id, &state)
            {
                error!(
                    "Task {} checkpoint {}: local persist failed: {}",
                    task_id, checkpoint_id, e
                );
            }

            // 2. S3 direct write (storage type = s3): Java external-storage
            //    model — failures log and skip (local disk already holds
            //    the state; restore falls back to it).
            if let Some(store) = &self.s3_store {
                store.save(job_id, task_id, checkpoint_id, &state).await;
            }

            // 3. Master notification; bytes ride along for the
            //    master-backed shared store (storage type = master).
            let upload = if self.upload_to_master { state.clone() } else { Vec::new() };
            let mut guard = self.master_client.lock().await;
            if let Some(client) = guard.as_mut() {
                let report = CheckpointReport {
                    job_id: job_id.to_string(),
                    task_id: task_id.to_string(),
                    checkpoint_id: checkpoint_id as i64,
                    timestamp,
                    checkpoint_data: upload,
                    success: true,
                };
                if let Err(e) = client.report_checkpoint(tonic::Request::new(report)).await {
                    warn!("report_checkpoint failed: {}", e);
                }
            }
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tmp_store(tag: &str) -> Arc<LocalStateStore> {
        let dir = std::env::temp_dir().join(format!("st-worker-{}", tag));
        let _ = std::fs::remove_dir_all(&dir);
        Arc::new(LocalStateStore::new(dir))
    }

    #[test]
    fn test_worker_node_creation() {
        let worker = WorkerNode::new("w1", "127.0.0.1:5001", tmp_store("create"));
        assert_eq!(worker.worker_id, "w1");
    }

    #[tokio::test]
    async fn test_assign_and_complete_fake_job() {
        let worker = Arc::new(WorkerNode::new("w1", "127.0.0.1:5001", tmp_store("run")));
        let mut config = HashMap::new();
        config.insert("source.plugin".to_string(), "Fake".to_string());
        config.insert(
            "source.config".to_string(),
            serde_json::json!({ "row.num": 3 }).to_string(),
        );
        config.insert("sink.plugin".to_string(), "Console".to_string());
        config.insert("sink.config".to_string(), "{}".to_string());
        config.insert("transform.config".to_string(), "[]".to_string());
        config.insert("checkpoint.interval".to_string(), "60000".to_string());
        let task = TaskDescriptor {
            task_id: "t-1".into(),
            job_id: "j-1".into(),
            stage_id: "s-1".into(),
            task_name: "pipeline".into(),
            task_index: 0,
            source_config_json: String::new(),
            sink_config_json: String::new(),
            parallelism: 1,
            config,
        };
        worker.assign_task(task).await;
        // Wait for detached execution to finish.
        tokio::time::sleep(std::time::Duration::from_millis(300)).await;
    }

    #[tokio::test]
    async fn test_cancel_stops_running_task() {
        let worker = Arc::new(WorkerNode::new("w1", "127.0.0.1:5001", tmp_store("cancel")));
        let mut config = HashMap::new();
        config.insert("source.plugin".to_string(), "Kafka".to_string());
        config.insert(
            "source.config".to_string(),
            serde_json::json!({ "bootstrap.servers": "127.0.0.1:19092", "topic": "never" })
                .to_string(),
        );
        config.insert("sink.plugin".to_string(), "Console".to_string());
        config.insert("sink.config".to_string(), "{}".to_string());
        config.insert("transform.config".to_string(), "[]".to_string());
        config.insert("checkpoint.interval".to_string(), "60000".to_string());
        let task = TaskDescriptor {
            task_id: "t-c".into(),
            job_id: "j-c".into(),
            stage_id: "s".into(),
            task_name: "streaming".into(),
            task_index: 0,
            source_config_json: String::new(),
            sink_config_json: String::new(),
            parallelism: 1,
            config,
        };
        worker.assign_task(task).await;
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        worker.cancel_jobs(&["j-c".to_string()]).await;
        assert!(worker.running_task_count().await <= 1);
    }
}
