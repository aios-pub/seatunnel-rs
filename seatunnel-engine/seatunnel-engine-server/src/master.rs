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
    CheckpointReport, Empty, FetchCheckpointRequest, FetchCheckpointResponse, HeartbeatRequest,
    HeartbeatResponse, MasterService, TaskStatusReport, UnregisterWorkerRequest,
    WorkerRegistration, WorkerRegistrationResponse,
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
    /// Advertised slot budget (0 = legacy/unlimited).
    pub slots: u32,
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

/// Identity of this master node, shared by Master/Client handlers so the
/// wire protocol can carry a real leader address and role.
#[derive(Debug, Clone)]
pub struct MasterInfo {
    /// Address other nodes should use to reach this master (advertise
    /// address, not the bind wildcard).
    pub advertise_addr: String,
    /// Deployment role: master | hybrid.
    pub role: String,
}

/// Master node state.
#[derive(Default)]
pub struct MasterState {
    /// Task assignments: task_id → worker_id.
    pub task_assignments: HashMap<String, String>,
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
    /// worker_id → task ids to preempt on its next heartbeat (fencing).
    pending_preemptions: Mutex<HashMap<String, Vec<String>>>,
    info: MasterInfo,
    /// Configured worker heartbeat period, echoed via `next_interval_ms`.
    heartbeat_interval_ms: u64,
    /// Soft liveness threshold: a worker silent longer than this gets no
    /// new assignments until it proves liveness again (still registered).
    worker_soft_timeout_ms: u64,
}

impl MasterHandler {
    pub fn new(
        coordinator: Arc<JobCoordinator>,
        workers: WorkerRegistry,
        info: MasterInfo,
        heartbeat_interval_ms: u64,
        worker_soft_timeout_ms: u64,
    ) -> Self {
        MasterHandler {
            state: Mutex::new(MasterState::default()),
            coordinator,
            workers,
            pending_preemptions: Mutex::new(HashMap::new()),
            info,
            heartbeat_interval_ms: heartbeat_interval_ms.clamp(250, 60_000),
            worker_soft_timeout_ms: worker_soft_timeout_ms.max(1_000),
        }
    }

    pub fn coordinator(&self) -> &Arc<JobCoordinator> {
        &self.coordinator
    }

    pub fn worker_registry(&self) -> &WorkerRegistry {
        &self.workers
    }

    pub fn info(&self) -> &MasterInfo {
        &self.info
    }
}

#[tonic::async_trait]
impl MasterService for MasterHandler {
    async fn register_worker(
        &self,
        request: Request<WorkerRegistration>,
    ) -> Result<Response<WorkerRegistrationResponse>, Status> {
        let reg = request.into_inner();
        info!(
            "Worker {} registering at {} ({} running task(s), slots={})",
            reg.worker_id,
            reg.address,
            reg.running_task_ids.len(),
            reg.slots
        );
        // Adopt-first: tasks still assigned to this worker are re-marked
        // Running for it (re-attach); only reassigned tasks get fenced.
        if !reg.running_task_ids.is_empty() {
            let preempted = self
                .coordinator
                .register_running_tasks(&reg.worker_id, &reg.running_task_ids);
            if !preempted.is_empty() {
                warn!(
                    "Worker {} returned with {} reassigned task(s); fencing",
                    reg.worker_id,
                    preempted.len()
                );
                self.pending_preemptions
                    .lock()
                    .await
                    .entry(reg.worker_id.clone())
                    .or_default()
                    .extend(preempted);
            }
        }
        self.workers.write().unwrap().insert(
            reg.worker_id.clone(),
            WorkerEntry {
                address: reg.address.clone(),
                last_heartbeat_ms: seatunnel_engine_core::now_millis(),
                slots: reg.slots,
            },
        );

        Ok(Response::new(WorkerRegistrationResponse {
            success: true,
            message: "registered".to_string(),
            leader_address: self.info.advertise_addr.clone(),
            term: self.coordinator.term(),
        }))
    }

    async fn heartbeat(
        &self,
        request: Request<HeartbeatRequest>,
    ) -> Result<Response<HeartbeatResponse>, Status> {
        let hb = request.into_inner();
        let worker_id = hb.worker_id.clone();
        let now = seatunnel_engine_core::now_millis();
        let my_term = self.coordinator.term();

        // Fencing: a worker operating under a HIGHER term means a
        // successor master exists — ratchet our term so the successor's
        // state wins on merge, and stop dispatching until we are sure we
        // are current (empty instruction set this round).
        if hb.term > my_term {
            warn!(
                "Worker {} reports term {} > ours {} — possible stale master; standing down \
                 dispatch this round",
                worker_id, hb.term, my_term
            );
            self.coordinator.observe_term(hb.term);
            return Ok(Response::new(HeartbeatResponse {
                worker_id,
                next_interval_ms: self.heartbeat_interval_ms as i64,
                pending_tasks: Vec::new(),
                cancel_jobs: Vec::new(),
                preempted_task_ids: Vec::new(),
                term: self.coordinator.term(),
                leader_hint: String::new(),
            }));
        }

        // A worker evicted by TTL that comes back (SIGSTOP/network
        // glitch) may still run tasks that were reassigned in its
        // absence — fence them from its heartbeat task list.
        {
            let known = {
                let reg = self.workers.read().unwrap();
                reg.contains_key(&worker_id)
            };
            if !known && !hb.tasks.is_empty() {
                let running: Vec<String> = hb.tasks.iter().map(|t| t.task_id.clone()).collect();
                let preempted = self
                    .coordinator
                    .register_running_tasks(&worker_id, &running);
                if !preempted.is_empty() {
                    warn!(
                        "Worker {} returned with {} reassigned task(s); fencing via heartbeat",
                        worker_id,
                        preempted.len()
                    );
                    self.pending_preemptions
                        .lock()
                        .await
                        .entry(worker_id.clone())
                        .or_default()
                        .extend(preempted);
                }
            }
        }

        // Live per-task metrics shipped with the heartbeat: record
        // counters, last-record timestamp and log increments.
        for task in &hb.tasks {
            self.coordinator.report_task_metrics(
                &task.task_id,
                task.processed_records.max(0) as u64,
                task.last_record_at,
                task.logs.clone(),
                &hb.worker_id,
                task.last_checkpoint_id.max(0) as u64,
                task.last_checkpoint_size_bytes.max(0) as u64,
            );
        }

        // Refresh liveness; a worker returning from a silence longer than
        // the soft timeout gets no NEW assignments this round (it may
        // still be recovering — running tasks are untouched).
        let mut soft_stale = false;
        {
            let mut reg = self.workers.write().unwrap();
            match reg.get_mut(&worker_id) {
                Some(entry) => {
                    soft_stale = now - entry.last_heartbeat_ms > self.worker_soft_timeout_ms as i64;
                    entry.last_heartbeat_ms = now;
                }
                None => {
                    // Heartbeat before registration — accept it anyway so a
                    // restarted worker recovers without a full re-register.
                    reg.insert(
                        worker_id.clone(),
                        WorkerEntry {
                            address: hb.address.clone(),
                            last_heartbeat_ms: now,
                            slots: 0,
                        },
                    );
                }
            }
        }
        if soft_stale {
            warn!(
                "Worker {} silent > {}ms (soft timeout): skipping new assignments this round",
                worker_id, self.worker_soft_timeout_ms
            );
        }

        // Failover-aware handout: pending tasks assigned to this worker
        // PLUS orphaned tasks of evicted workers (reassigned here).
        let pending_tasks = if soft_stale {
            Vec::new()
        } else {
            let live = self.workers.read().unwrap();
            self.coordinator
                .claim_tasks_for_worker(&worker_id, &hb.address, &|w| live.contains_key(w))
        };
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

        // Preemption fence: tasks reassigned away from this worker.
        let preempted_task_ids = self
            .pending_preemptions
            .lock()
            .await
            .remove(&worker_id)
            .unwrap_or_default();

        Ok(Response::new(HeartbeatResponse {
            worker_id,
            next_interval_ms: self.heartbeat_interval_ms as i64,
            pending_tasks,
            cancel_jobs,
            preempted_task_ids,
            term: self.coordinator.term(),
            // Empty hint = this node is the active master.
            leader_hint: String::new(),
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
            "Checkpoint {} for job {} task {} success={} ({} bytes)",
            report.checkpoint_id,
            report.job_id,
            report.task_id,
            report.success,
            report.checkpoint_data.len()
        );
        // Master-backed shared store: persist the uploaded bytes so any
        // worker can resume this task after a failover.
        if report.success && !report.checkpoint_data.is_empty() {
            self.coordinator
                .checkpoint_store()
                .save(
                    &report.job_id,
                    &report.task_id,
                    report.checkpoint_id.max(0) as u64,
                    &report.checkpoint_data,
                )
                .await;
        }
        self.coordinator.report_checkpoint(
            &report.job_id,
            &report.task_id,
            report.checkpoint_id.max(0) as u64,
            report.success,
        );
        Ok(Response::new(Empty {}))
    }

    async fn fetch_checkpoint(
        &self,
        request: Request<FetchCheckpointRequest>,
    ) -> Result<Response<FetchCheckpointResponse>, Status> {
        let req = request.into_inner();
        match self
            .coordinator
            .fetch_checkpoint(&req.job_id, &req.task_id)
            .await
        {
            Some((id, data)) => Ok(Response::new(FetchCheckpointResponse {
                checkpoint_id: id as i64,
                checkpoint_data: data,
            })),
            None => Ok(Response::new(FetchCheckpointResponse {
                checkpoint_id: 0,
                checkpoint_data: Vec::new(),
            })),
        }
    }

    async fn unregister_worker(
        &self,
        request: Request<UnregisterWorkerRequest>,
    ) -> Result<Response<Empty>, Status> {
        let req = request.into_inner();
        warn!("Worker unregistering: {}", req.worker_id);
        self.workers.write().unwrap().remove(&req.worker_id);
        // Graceful shutdown must actually RELEASE the worker's tasks for
        // failover — without this a clean restart waits for the hard
        // eviction timeout before another worker can take over.
        let affected = self.coordinator.evict_worker(&req.worker_id);
        if !affected.is_empty() {
            info!(
                "Worker {} unregistered: {} task(s) released for takeover",
                req.worker_id,
                affected.len()
            );
        }
        Ok(Response::new(Empty {}))
    }
}

/// Master-to-master state replication endpoint (HA standby sync).
pub struct ReplicationHandler {
    coordinator: Arc<JobCoordinator>,
}

impl ReplicationHandler {
    pub fn new(coordinator: Arc<JobCoordinator>) -> Self {
        ReplicationHandler { coordinator }
    }
}

#[tonic::async_trait]
impl seatunnel_engine_comm::ReplicationService for ReplicationHandler {
    async fn pull_state(
        &self,
        request: Request<seatunnel_engine_comm::PullStateRequest>,
    ) -> Result<Response<seatunnel_engine_comm::StateSnapshot>, Status> {
        let req = request.into_inner();
        tracing::debug!("Replication: state pulled by {}", req.requester_id);
        let state = self.coordinator.export_state().await;
        let snapshot = seatunnel_engine_comm::StateSnapshot {
            state_json: serde_json::to_string(&state)
                .map_err(|e| Status::internal(format!("serialize state: {}", e)))?,
            exported_at_ms: seatunnel_engine_core::now_millis(),
        };
        Ok(Response::new(snapshot))
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
                slots: 8,
            },
        );
        assert_eq!(
            registry_snapshot(&registry),
            vec![("w1".to_string(), "127.0.0.1:5001".to_string())]
        );
    }
}
