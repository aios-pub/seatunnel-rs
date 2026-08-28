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

//! Client-facing gRPC service handler.
//!
//! Implements `ClientService`: job submission, cancellation, status queries
//! and cluster introspection for the CLI and REST clients.

use crate::job_coordinator::{Command, JobCoordinator};
use crate::master::{MasterInfo, WorkerRegistry, registry_snapshot_admission};
use crate::raft::WritePath;
use seatunnel_engine_comm::{
    CancelJobRequest, CheckpointEntry, ClusterInfo, Empty, JobCheckpointHistory, JobList, JobLogs,
    JobStatus, JobStatusRequest, JobSummary, SubmitJobRequest, SubmitJobResponse,
    TaskCheckpointHistory, TaskLogs, WorkerInfo,
};
use std::collections::HashMap;
use std::sync::Arc;
use tonic::{Request, Response, Status};

/// Client service handler backed by shared coordinator + worker registry.
#[derive(Clone)]
pub struct ClientHandler {
    coordinator: Arc<JobCoordinator>,
    workers: WorkerRegistry,
    info: MasterInfo,
    writes: Arc<dyn WritePath>,
    /// Long-poll wake signal (shared with the master handler): a fresh
    /// submission must unpause parked worker heartbeats immediately.
    wake: Arc<tokio::sync::Notify>,
}

impl ClientHandler {
    pub fn new(
        coordinator: Arc<JobCoordinator>,
        workers: WorkerRegistry,
        info: MasterInfo,
        writes: Arc<dyn WritePath>,
        wake: Arc<tokio::sync::Notify>,
    ) -> Self {
        ClientHandler {
            coordinator,
            workers,
            info,
            writes,
            wake,
        }
    }

    /// Convenience constructor for tests / embedded setups.
    pub fn new_direct(
        coordinator: Arc<JobCoordinator>,
        workers: WorkerRegistry,
        info: MasterInfo,
    ) -> Self {
        Self::new(
            coordinator.clone(),
            workers,
            info,
            Arc::new(crate::raft::DirectWrite::new(coordinator)),
            Arc::new(tokio::sync::Notify::new()),
        )
    }

    pub fn coordinator(&self) -> &Arc<JobCoordinator> {
        &self.coordinator
    }

    fn worker_infos(&self) -> Vec<WorkerInfo> {
        // Live per-worker running-task counts from the coordinator's
        // view of assignments.
        let mut running_per_worker: HashMap<String, i32> = HashMap::new();
        for job in self.coordinator.list_jobs() {
            for info in job.tasks.values() {
                if info.state == crate::job_coordinator::JobState::Running
                    && !info.worker_id.is_empty()
                {
                    *running_per_worker.entry(info.worker_id.clone()).or_default() += 1;
                }
            }
        }
        self.workers
            .read()
            .unwrap()
            .iter()
            .map(|(id, e)| WorkerInfo {
                worker_id: id.clone(),
                address: e.address.clone(),
                last_heartbeat: e.last_heartbeat_ms,
                running_tasks: running_per_worker.get(id).copied().unwrap_or(0),
                load_score: e.load_score,
                lag_ms: e.lag_ms,
                mem_permille: e.mem_permille,
                can_accept: e.can_accept,
                ..Default::default()
            })
            .collect()
    }
}

#[tonic::async_trait]
impl seatunnel_engine_comm::ClientService for ClientHandler {
    async fn submit_job(
        &self,
        request: Request<SubmitJobRequest>,
    ) -> Result<Response<SubmitJobResponse>, Status> {
        let req = request.into_inner();
        let job_id = req.job_id.clone();
        let job_name = if req.job_name.is_empty() {
            format!("job-{}", job_id)
        } else {
            req.job_name.clone()
        };
        let parallelism_override = (req.parallelism > 0).then_some(req.parallelism as usize);

        tracing::info!(
            "Submitting job {}: name='{}' parallelism-override={:?}",
            job_id,
            job_name,
            parallelism_override
        );

        let config: serde_json::Value = serde_json::from_slice(&req.job_config).map_err(|e| {
            tracing::error!("Invalid job config: {}", e);
            Status::invalid_argument(format!("invalid job config: {}", e))
        })?;
        // Leadership gate: only the leader accepts submissions.
        if !self.writes.is_leader() {
            let hint = self.writes.leader_hint();
            return Err(Status::failed_precondition(format!(
                "not the leader; retry at {}",
                if hint.is_empty() { "another master" } else { &hint }
            )));
        }
        let workers = registry_snapshot_admission(&self.workers);
        let (job, descriptors, _tasks) = self
            .coordinator
            .plan_job(&job_id, &job_name, &config, parallelism_override, &workers)
            .map_err(|e| {
                tracing::error!("Job {} rejected: {}", job_id, e);
                Status::failed_precondition(e.to_string())
            })?;
        let task_count = descriptors.len();
        let scheduled_id = job.job_id.clone();
        let cmd = Command::SubmitJob {
            job,
            descriptors,
        };
        self.writes
            .propose(cmd)
            .await
            .map_err(|e| Status::failed_precondition(format!("consensus write: {}", e)))?;
        // New tasks exist: parked long-poll heartbeats must wake now.
        self.wake.notify_waiters();
        let tasks = task_count;

        tracing::info!(
            "Job {} scheduled: {} chained task(s) across {} worker(s)",
            scheduled_id,
            tasks,
            workers.len()
        );

        Ok(Response::new(SubmitJobResponse {
            success: true,
            job_id: scheduled_id,
            message: format!("job '{}' scheduled with {} task(s)", job_name, tasks),
        }))
    }

    async fn cancel_job(
        &self,
        request: Request<CancelJobRequest>,
    ) -> Result<Response<Empty>, Status> {
        let req = request.into_inner();
        let job_id = req.job_id;
        tracing::info!("Cancelling job {}", job_id);

        let cancelled = self
            .coordinator
            .get_job(&job_id)
            .map(|j| !j.state.is_terminal())
            .unwrap_or(false);
        if !cancelled {
            return Err(Status::not_found(format!(
                "job {} not found or already terminal",
                job_id
            )));
        }
        let cmd = Command::CancelJob {
            job_id: job_id.clone(),
            at_ms: seatunnel_engine_core::now_millis(),
        };
        self.writes
            .propose(cmd)
            .await
            .map_err(|e| Status::failed_precondition(format!("consensus write: {}", e)))?;
        self.wake.notify_waiters();
        Ok(Response::new(Empty {}))
    }

    async fn get_job_status(
        &self,
        request: Request<JobStatusRequest>,
    ) -> Result<Response<JobStatus>, Status> {
        let req = request.into_inner();
        let job_id = req.job_id;

        let Some(job) = self.coordinator.get_job(&job_id) else {
            return Err(Status::not_found(format!("Job {} not found", job_id)));
        };

        let mut tasks: Vec<seatunnel_engine_comm::TaskStatusInfo> = job
            .tasks
            .values()
            .map(|info| seatunnel_engine_comm::TaskStatusInfo {
                task_id: info.task_id.clone(),
                stage_id: info.stage_id.clone(),
                state: match &info.state {
                    crate::job_coordinator::JobState::Created => 1,
                    crate::job_coordinator::JobState::Scheduled => 1,
                    crate::job_coordinator::JobState::Deploying => 2,
                    crate::job_coordinator::JobState::Running => 2,
                    crate::job_coordinator::JobState::Completed => 3,
                    crate::job_coordinator::JobState::Failed { .. } => 4,
                    crate::job_coordinator::JobState::Cancelled => 5,
                },
                processed_records: info.processed_records as i64,
                start_time: job.start_time,
                end_time: job.end_time.unwrap_or(0),
                last_record_at: info.last_record_at,
                worker_id: info.worker_id.clone(),
            })
            .collect();
        tasks.sort_by(|a, b| a.task_id.cmp(&b.task_id));

        Ok(Response::new(JobStatus {
            job_id,
            state: job.state.to_proto_state(),
            job_name: job.job_name,
            start_time: job.start_time,
            end_time: job.end_time.unwrap_or(0),
            error_message: job.error_message.unwrap_or_default(),
            tasks,
            checkpoint_interval_ms: job.checkpoint_interval_ms as i64,
            checkpoints_completed: job.checkpoints_completed as i64,
            job_config: job.raw_config.clone(),
        }))
    }

    async fn get_cluster_info(
        &self,
        _request: Request<Empty>,
    ) -> Result<Response<ClusterInfo>, Status> {
        let workers = self.worker_infos();
        let jobs = self.coordinator.list_jobs();
        let total_tasks: usize = jobs.iter().map(|j| j.tasks.len()).sum();
        let running_tasks: usize = jobs
            .iter()
            .flat_map(|j| j.tasks.values())
            .filter(|t| t.state == crate::job_coordinator::JobState::Running)
            .count();

        Ok(Response::new(ClusterInfo {
            available_workers: workers.len() as i32,
            workers,
            total_tasks: total_tasks as i32,
            running_tasks: running_tasks as i32,
            leader_id: self.info.advertise_addr.clone(),
            term: self.coordinator.term(),
            leader_address: self.info.advertise_addr.clone(),
            role: self.info.role.clone(),
        }))
    }

    async fn list_jobs(&self, _request: Request<Empty>) -> Result<Response<JobList>, Status> {
        let mut summaries: Vec<JobSummary> = self
            .coordinator
            .list_jobs()
            .into_iter()
            .map(|j| JobSummary {
                job_id: j.job_id,
                job_name: j.job_name,
                state: j.state.to_proto_state(),
                start_time: j.start_time,
                end_time: j.end_time.unwrap_or(0),
            })
            .collect();
        summaries.sort_by_key(|j| std::cmp::Reverse(j.start_time));
        Ok(Response::new(JobList { jobs: summaries }))
    }

    async fn get_job_checkpoints(
        &self,
        request: Request<JobStatusRequest>,
    ) -> Result<Response<JobCheckpointHistory>, Status> {
        let req = request.into_inner();
        let job_id = req.job_id;

        let Some(job) = self.coordinator.get_job(&job_id) else {
            return Err(Status::not_found(format!("Job {} not found", job_id)));
        };

        // Full history from the master-backed store when present; every
        // other storage backend falls back to the most recent checkpoint
        // reported via heartbeats, so the console always shows progress.
        let mut tasks: Vec<TaskCheckpointHistory> = self
            .coordinator
            .checkpoint_store()
            .list_job_meta(&job_id)
            .await
            .into_iter()
            .map(|t| TaskCheckpointHistory {
                task_id: t.task_id,
                entries: t
                    .entries
                    .into_iter()
                    .map(|e| CheckpointEntry {
                        checkpoint_id: e.checkpoint_id as i64,
                        size_bytes: e.size_bytes as i64,
                    })
                    .collect(),
            })
            .collect();
        for info in job.tasks.values() {
            if info.last_checkpoint_id > 0
                && !tasks
                    .iter()
                    .any(|t| t.task_id == info.task_id && !t.entries.is_empty())
            {
                tasks.push(TaskCheckpointHistory {
                    task_id: info.task_id.clone(),
                    entries: vec![CheckpointEntry {
                        checkpoint_id: info.last_checkpoint_id as i64,
                        size_bytes: info.last_checkpoint_size as i64,
                    }],
                });
            }
        }
        tasks.sort_by(|a, b| a.task_id.cmp(&b.task_id));

        Ok(Response::new(JobCheckpointHistory {
            job_id,
            checkpoint_interval_ms: job.checkpoint_interval_ms as i64,
            checkpoints_completed: job.checkpoints_completed as i64,
            tasks,
        }))
    }

    async fn get_job_logs(
        &self,
        request: Request<JobStatusRequest>,
    ) -> Result<Response<JobLogs>, Status> {
        let req = request.into_inner();
        let job_id = req.job_id;

        let Some(job) = self.coordinator.get_job(&job_id) else {
            return Err(Status::not_found(format!("Job {} not found", job_id)));
        };

        let mut tasks: Vec<TaskLogs> = job
            .tasks
            .values()
            .map(|t| TaskLogs {
                task_id: t.task_id.clone(),
                lines: t.logs.iter().cloned().collect(),
            })
            .collect();
        tasks.sort_by(|a, b| a.task_id.cmp(&b.task_id));

        Ok(Response::new(JobLogs { job_id, tasks }))
    }
}
