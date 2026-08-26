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

//! Master service handler: worker registry, task dispatch over heartbeat,
//! status/checkpoint ingestion.

use std::collections::HashMap;
use std::sync::Arc;

use seatunnel_engine_comm::{
    CheckpointReport, Empty, HeartbeatRequest, HeartbeatResponse, MasterService, TaskDescriptor,
    TaskStatusReport, UnregisterWorkerRequest, WorkerRegistration, WorkerRegistrationResponse,
};
use tokio::sync::Mutex;
use tonic::{Request, Response, Status};
use tracing::{info, warn};

use crate::job_coordinator::{JobCoordinator, JobState};

/// A registered worker's address and liveness.
#[derive(Debug, Clone)]
pub struct WorkerEntry {
    pub address: String,
    pub last_heartbeat_ms: i64,
}

/// Shared between MasterService (registration/heartbeats) and ClientService
/// (scheduling decisions) so submissions always see live workers.
pub type WorkerRegistry = Arc<std::sync::RwLock<HashMap<String, WorkerEntry>>>;

pub fn new_worker_registry() -> WorkerRegistry {
    Arc::new(std::sync::RwLock::new(HashMap::new()))
}

/// Snapshot of registered workers as `(id, address)` pairs.
pub fn registry_snapshot(registry: &WorkerRegistry) -> Vec<(String, String)> {
    registry
        .read()
        .unwrap()
        .iter()
        .map(|(id, e)| (id.clone(), e.address.clone()))
        .collect()
}

/// Master node state.
#[derive(Default)]
pub struct MasterState {
    /// Task assignments: task_id → worker_id.
    pub task_assignments: HashMap<String, String>,
    pub leader_id: Option<String>,
}

#[derive(Debug, Clone)]
pub struct JobInfo {
    pub job_id: String,
    pub job_name: String,
    pub state: String,
    pub parallelism: i32,
    pub start_time: i64,
    pub end_time: Option<i64>,
    pub error_message: Option<String>,
}

/// Master service handler with real job coordination.
pub struct MasterHandler {
    state: Mutex<MasterState>,
    coordinator: Arc<JobCoordinator>,
    workers: WorkerRegistry,
}

impl MasterHandler {
    pub fn new(coordinator: Arc<JobCoordinator>, workers: WorkerRegistry) -> Self {
        MasterHandler {
            state: Mutex::new(MasterState::default()),
            coordinator,
            workers,
        }
    }

    pub fn coordinator(&self) -> &Arc<JobCoordinator> {
        &self.coordinator
    }

    pub fn worker_registry(&self) -> &WorkerRegistry {
        &self.workers
    }

    #[cfg(test)]
    pub async fn leader_id(&self) -> Option<String> {
        self.state.lock().await.leader_id.clone()
    }
}

#[tonic::async_trait]
impl MasterService for MasterHandler {
    async fn register_worker(
        &self,
        request: Request<WorkerRegistration>,
    ) -> Result<Response<WorkerRegistrationResponse>, Status> {
        let reg = request.into_inner();
        info!("Worker registering: {} at {}", reg.worker_id, reg.address);
        self.workers.write().unwrap().insert(
            reg.worker_id.clone(),
            WorkerEntry {
                address: reg.address.clone(),
                last_heartbeat_ms: seatunnel_engine_core::now_millis(),
            },
        );
        let mut state = self.state.lock().await;
        state.task_assignments.retain(|_, _| true);

        Ok(Response::new(WorkerRegistrationResponse {
            success: true,
            message: "registered".to_string(),
            leader_address: state.leader_id.clone().unwrap_or_default(),
        }))
    }

    async fn heartbeat(
        &self,
        request: Request<HeartbeatRequest>,
    ) -> Result<Response<HeartbeatResponse>, Status> {
        let hb = request.into_inner();
        let worker_id = hb.worker_id.clone();

        // Refresh liveness.
        {
            let mut reg = self.workers.write().unwrap();
            match reg.get_mut(&worker_id) {
                Some(entry) => entry.last_heartbeat_ms = seatunnel_engine_core::now_millis(),
                None => {
                    // Heartbeat before registration — accept it anyway so a
                    // restarted worker recovers without a full re-register.
                    reg.insert(
                        worker_id.clone(),
                        WorkerEntry {
                            address: hb.address.clone(),
                            last_heartbeat_ms: seatunnel_engine_core::now_millis(),
                        },
                    );
                }
            }
        }

        // Hand out never-dispatched tasks, then mark them as deploying so
        // they cannot be double-assigned on the next heartbeat.
        let pending_tasks: Vec<TaskDescriptor> =
            self.coordinator.get_pending_tasks_for_worker(&worker_id);
        if !pending_tasks.is_empty() {
            info!(
                "Dispatching {} task(s) to worker {}",
                pending_tasks.len(),
                worker_id
            );
            let ids: Vec<String> = pending_tasks.iter().map(|t| t.task_id.clone()).collect();
            self.coordinator.mark_tasks_dispatched(&ids, &worker_id);
            let mut state = self.state.lock().await;
            for id in &ids {
                state.task_assignments.insert(id.clone(), worker_id.clone());
            }
        }

        // Push cancellations so workers stop their local tasks promptly.
        let cancel_jobs = self.coordinator.cancelled_job_ids();

        Ok(Response::new(HeartbeatResponse {
            worker_id,
            next_interval_ms: 2000,
            pending_tasks,
            cancel_jobs,
        }))
    }

    async fn report_task_status(
        &self,
        request: Request<TaskStatusReport>,
    ) -> Result<Response<Empty>, Status> {
        let report = request.into_inner();
        let mut state = self.state.lock().await;
        state.task_assignments.remove(&report.task_id);
        drop(state);

        let state_str = match report.state {
            0 | 1 => "CREATED",
            2 => "RUNNING",
            3 => "COMPLETED",
            4 => "FAILED",
            5 => "CANCELLED",
            _ => "UNKNOWN",
        };
        info!(
            "Task {} reported {} (records={})",
            report.task_id,
            JobState::from(state_str).to_wire(),
            report.processed_records
        );
        self.coordinator.report_task_status(
            &report.job_id,
            &report.task_id,
            state_str,
            report.processed_records.max(0) as u64,
            if report.error_message.is_empty() {
                None
            } else {
                Some(report.error_message)
            },
        );

        Ok(Response::new(Empty {}))
    }

    async fn report_checkpoint(
        &self,
        request: Request<CheckpointReport>,
    ) -> Result<Response<Empty>, Status> {
        let report = request.into_inner();
        tracing::debug!(
            "Checkpoint {} for job {} task {} success={}",
            report.checkpoint_id,
            report.job_id,
            report.task_id,
            report.success
        );
        self.coordinator.report_checkpoint(
            &report.job_id,
            &report.task_id,
            report.checkpoint_id.max(0) as u64,
            report.success,
        );
        Ok(Response::new(Empty {}))
    }

    async fn unregister_worker(
        &self,
        request: Request<UnregisterWorkerRequest>,
    ) -> Result<Response<Empty>, Status> {
        let req = request.into_inner();
        warn!("Worker unregistering: {}", req.worker_id);
        self.workers.write().unwrap().remove(&req.worker_id);
        Ok(Response::new(Empty {}))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_registry_roundtrip() {
        let registry = new_worker_registry();
        assert!(registry_snapshot(&registry).is_empty());
        registry.write().unwrap().insert(
            "w1".into(),
            WorkerEntry {
                address: "127.0.0.1:5001".into(),
                last_heartbeat_ms: 0,
            },
        );
        assert_eq!(
            registry_snapshot(&registry),
            vec![("w1".to_string(), "127.0.0.1:5001".to_string())]
        );
    }
}
