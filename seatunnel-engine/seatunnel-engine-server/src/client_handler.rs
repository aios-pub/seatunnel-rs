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

use crate::job_coordinator::JobCoordinator;
use crate::master::{registry_snapshot, WorkerRegistry};
use seatunnel_engine_comm::{
    CancelJobRequest, ClusterInfo, Empty, JobList, JobStatus, JobStatusRequest, JobSummary,
    SubmitJobRequest, SubmitJobResponse, WorkerInfo,
};
use std::sync::Arc;
use tonic::{Request, Response, Status};

/// Client service handler backed by shared coordinator + worker registry.
#[derive(Clone)]
pub struct ClientHandler {
    coordinator: Arc<JobCoordinator>,
    workers: WorkerRegistry,
}

impl ClientHandler {
    pub fn new(coordinator: Arc<JobCoordinator>, workers: WorkerRegistry) -> Self {
        ClientHandler { coordinator, workers }
    }

    pub fn coordinator(&self) -> &Arc<JobCoordinator> {
        &self.coordinator
    }

    fn worker_infos(&self) -> Vec<WorkerInfo> {
        self.workers
            .read()
            .unwrap()
            .iter()
            .map(|(id, e)| WorkerInfo {
                worker_id: id.clone(),
                address: e.address.clone(),
                last_heartbeat: e.last_heartbeat_ms,
                ..Default::default()
            })
            .collect()
    }
}

fn job_state_to_proto(state: &str) -> i32 {
    match state {
        "RUNNING" => 3,
        "COMPLETED" => 4,
        "FAILED" => 5,
        "CANCELLED" => 6,
        _ => 1,
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
        let parallelism_override = (req.parallelism > 0).then(|| req.parallelism as usize);

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
        let workers = registry_snapshot(&self.workers);
        let (scheduled_id, tasks) = self
            .coordinator
            .compile_and_schedule(&job_id, &job_name, &config, parallelism_override, &workers)
            .map_err(|e| {
                tracing::error!("Job {} rejected: {}", job_id, e);
                Status::failed_precondition(e.to_string())
            })?;

        info!(
            "Job {} scheduled: {} chained task(s) across {} worker(s)",
            scheduled_id,
            tasks.len(),
            workers.len()
        );

        Ok(Response::new(SubmitJobResponse {
            success: true,
            job_id: scheduled_id,
            message: format!(
                "job '{}' scheduled with {} task(s)",
                job_name,
                tasks.len()
            ),
        }))
    }

    async fn cancel_job(
        &self,
        request: Request<CancelJobRequest>,
    ) -> Result<Response<Empty>, Status> {
        let req = request.into_inner();
        let job_id = req.job_id;
        tracing::info!("Cancelling job {}", job_id);

        if !self.coordinator.cancel_job(&job_id) {
            return Err(Status::not_found(format!(
                "job {} not found or already terminal",
                job_id
            )));
        }
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
        }))
    }

    async fn get_cluster_info(
        &self,
        _request: Request<Empty>,
    ) -> Result<Response<ClusterInfo>, Status> {
        let workers = self.worker_infos();
        let jobs = self.coordinator.list_jobs();
        let total_tasks: usize = jobs.iter().map(|j| j.tasks.len()).sum();
        let running_tasks: usize = jobs.iter().map(|j| j.tasks.len()).sum();

        Ok(Response::new(ClusterInfo {
            available_workers: workers.len() as i32,
            workers,
            total_tasks: total_tasks as i32,
            running_tasks: running_tasks as i32,
            leader_id: "self".to_string(),
        }))
    }

    async fn list_jobs(
        &self,
        _request: Request<Empty>,
    ) -> Result<Response<JobList>, Status> {
        let mut summaries: Vec<JobSummary> = self
            .coordinator
            .list_jobs()
            .into_iter()
            .map(|j| JobSummary {
                job_id: j.job_id,
                job_name: j.job_name,
                state: j.state.to_proto_state(),
                start_time: j.start_time,
            })
            .collect();
        summaries.sort_by(|a, b| b.start_time.cmp(&a.start_time));
        Ok(Response::new(JobList { jobs: summaries }))
    }
}
