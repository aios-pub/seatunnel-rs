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
//! Implements `ClientService` so that the CLI can submit jobs and query cluster state.

use crate::job_manager::JobManager;
use parking_lot::RwLock;
use seatunnel_engine_comm::{
    CancelJobRequest, ClusterInfo, Empty, JobList, JobStatus, JobStatusRequest, JobSummary,
    SubmitJobRequest, SubmitJobResponse, WorkerInfo,
};
use std::collections::HashMap;
use std::sync::Arc;
use tonic::{Request, Response, Status};

/// Client service handler backed by a shared `JobManager` and worker state.
#[derive(Clone)]
pub struct ClientHandler {
    job_manager: Arc<JobManager>,
    workers: Arc<parking_lot::RwLock<Vec<WorkerInfo>>>,
    running_tasks: Arc<parking_lot::RwLock<usize>>,
}

impl ClientHandler {
    pub fn new(job_manager: Arc<JobManager>) -> Self {
        ClientHandler {
            job_manager,
            workers: Arc::new(parking_lot::RwLock::new(Vec::new())),
            running_tasks: Arc::new(parking_lot::RwLock::new(0)),
        }
    }

    /// Register a worker from the master heartbeat flow.
    pub fn register_worker(&self, id: &str, address: &str, _resources: &HashMap<String, String>) {
        let mut workers = self.workers.write();
            workers.push(WorkerInfo {
                worker_id: id.to_string(),
                address: address.to_string(),
                ..Default::default()
            });
    }

    /// Update the running task count.
    pub fn update_running_tasks(&self, delta: isize) {
        let mut count = self.running_tasks.write();
        *count = (*count as isize + delta).max(0) as usize;
    }
}

#[tonic::async_trait]
impl seatunnel_engine_comm::ClientService for ClientHandler {
    async fn submit_job(
        &self,
        request: Request<SubmitJobRequest>,
    ) -> Result<Response<SubmitJobResponse>, Status> {
        let req = request.into_inner();
        let job_id = req.job_id;
        let job_name = req.job_name;
        let parallelism = req.parallelism;
        let start_time = 0;

        tracing::info!(
            "Client submitting job: id={}, name='{}', parallelism={}",
            job_id,
            job_name,
            parallelism
        );

        // Persist job in the job manager
        self.job_manager
            .submit_job(job_id.clone(), job_name, parallelism, start_time);

        // TODO: actually schedule tasks to workers — this is the core orchestration gap

        Ok(Response::new(SubmitJobResponse {
            success: true,
            job_id,
            message: "job submitted".to_string(),
        }))
    }

    async fn cancel_job(
        &self,
        request: Request<CancelJobRequest>,
    ) -> Result<Response<Empty>, Status> {
        let req = request.into_inner();
        let job_id = req.job_id;
        tracing::info!("Client cancelling job: {}", job_id);
        self.job_manager.cancel_job(&job_id);
        Ok(Response::new(Empty {}))
    }

    async fn get_job_status(
        &self,
        request: Request<JobStatusRequest>,
    ) -> Result<Response<JobStatus>, Status> {
        let req = request.into_inner();
        let job_id = req.job_id;
        tracing::info!("Client requesting job status: {}", job_id);

        let job = match self.job_manager.get_job(&job_id) {
            Some(j) => j,
            None => {
                return Err(Status::not_found(format!("Job {} not found", job_id)));
            }
        };

        let state: i32 = match job.state.as_str() {
            "CREATED" => 0,
            "RUNNING" => 1,
            "COMPLETED" => 2,
            "FAILED" => 3,
            "CANCELLED" => 4,
            _ => 0,
        };

        Ok(Response::new(JobStatus {
            job_id,
            state,
            job_name: job.job_name,
            start_time: job.start_time,
            end_time: job.end_time.unwrap_or(0),
            error_message: job.error_message.clone().unwrap_or_default(),
            tasks: vec![], // populated during real task scheduling
        }))
    }

    async fn get_cluster_info(
        &self,
        _request: Request<Empty>,
    ) -> Result<Response<ClusterInfo>, Status> {
        let workers = self.workers.read();
        let running = *self.running_tasks.read();
        Ok(Response::new(ClusterInfo {
            workers: workers.clone(),
            total_tasks: 0,
            running_tasks: running as i32,
            available_workers: workers.len() as i32,
            leader_id: "self".to_string(),
        }))
    }

    async fn list_jobs(
        &self,
        _request: Request<Empty>,
    ) -> Result<Response<JobList>, Status> {
        let jobs = self.job_manager.list_jobs();
        let summaries: Vec<JobSummary> = jobs
            .into_iter()
            .map(|j| JobSummary {
                job_id: j.job_id,
                job_name: j.job_name,
                state: match j.state.as_str() {
                    "RUNNING" => 1,
                    "COMPLETED" => 2,
                    "FAILED" => 3,
                    "CANCELLED" => 4,
                    _ => 0,
                },
                start_time: j.start_time,
            })
            .collect();
        Ok(Response::new(JobList { jobs: summaries }))
    }
}
