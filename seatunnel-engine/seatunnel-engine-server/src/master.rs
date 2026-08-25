/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.
 */

use seatunnel_engine_comm::{
    CheckpointReport, Empty, HeartbeatRequest, HeartbeatResponse, MasterService, TaskDescriptor,
    TaskStatusReport, UnregisterWorkerRequest, WorkerRegistration, WorkerRegistrationResponse,
};
use std::collections::HashMap;
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};
use tokio::sync::Mutex;
use tonic::{Request, Response, Status};
use tracing::{error, info, warn};

/// Master node state.
pub struct MasterState {
    /// Registered workers: worker_id → address.
    pub workers: HashMap<String, String>,
    /// Running jobs.
    pub jobs: HashMap<String, JobInfo>,
    /// Task assignments: task_id → worker_id.
    pub task_assignments: HashMap<String, String>,
    /// Leader ID.
    pub leader_id: Option<String>,
    /// Pending tasks awaiting assignment: task_id → TaskDescriptor.
    pub pending_tasks: HashMap<String, TaskDescriptor>,
}

impl MasterState {
    pub fn new() -> Self {
        MasterState {
            workers: HashMap::new(),
            jobs: HashMap::new(),
            task_assignments: HashMap::new(),
            leader_id: None,
            pending_tasks: HashMap::new(),
        }
    }
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

/// Master service handler.
pub struct MasterHandler {
    state: Arc<Mutex<MasterState>>,
    next_task_index: AtomicUsize,
}

impl MasterHandler {
    pub fn new() -> Self {
        MasterHandler {
            state: Arc::new(Mutex::new(MasterState::new())),
            next_task_index: AtomicUsize::new(0),
        }
    }
    
    /// Add pending tasks to the scheduling queue.
    pub fn submit_tasks(&self, job_id: &str, tasks: Vec<TaskDescriptor>) {
        let mut state = self.state.blocking_lock();
        for task in tasks {
            state.pending_tasks.insert(task.task_id.clone(), task);
        }
        info!("Submitted {} tasks for job {}", tasks.len(), job_id);
    }

impl MasterHandler {
    pub fn new() -> Self {
        MasterHandler {
            state: Arc::new(Mutex::new(MasterState::new())),
            next_task_index: AtomicUsize::new(0),
        }
    }

    /// Add pending tasks to the scheduling queue.
    pub fn submit_tasks(&self, job_id: &str, tasks: Vec<TaskDescriptor>) {
        let mut state = self.state.blocking_lock();
        for task in tasks {
            state.pending_tasks.insert(task.task_id.clone(), task);
        }
        info!("Submitted {} tasks for job {}", tasks.len(), job_id);
    }
}

#[tonic::async_trait]
impl MasterService for MasterHandler {
    async fn register_worker(
        &self,
        request: Request<WorkerRegistration>,
    ) -> Result<Response<WorkerRegistrationResponse>, Status> {
        let reg = request.into_inner();
        let mut state = self.state.lock().await;

        info!("Worker registering: {} at {}", reg.worker_id, reg.address);
        state
            .workers
            .insert(reg.worker_id.clone(), reg.address.clone());

        Ok(Response::new(WorkerRegistrationResponse {
            success: true,
            message: "registered".to_string(),
            leader_address: reg.address.clone(),
        }))
    }

    async fn heartbeat(
        &self,
        request: Request<HeartbeatRequest>,
    ) -> Result<Response<HeartbeatResponse>, Status> {
        let hb = request.into_inner();
        let mut state = self.state.lock().await;

        // Update worker registration timestamp
        if let Some(addr) = state.workers.get(&hb.worker_id) {
            let _ = addr; // keep compilation clean
        }

        // Assign pending tasks to this worker
        let pending_tasks: Vec<TaskDescriptor> = state.pending_tasks.values().cloned().collect();
        if !pending_tasks.is_empty() {
            for task in &pending_tasks {
                state.task_assignments.insert(task.task_id.clone(), hb.worker_id.clone());
            }
            for task_id in pending_tasks.iter().map(|t| t.task_id.clone()).collect::<Vec<_>>() {
                state.pending_tasks.remove(&task_id);
            }
        }

        info!("Heartbeat from worker: {}", hb.worker_id);

        Ok(Response::new(HeartbeatResponse {
            worker_id: hb.worker_id,
            next_interval_ms: 5000,
            pending_tasks,
        }))
    }

    async fn report_task_status(
        &self,
        request: Request<TaskStatusReport>,
    ) -> Result<Response<Empty>, Status> {
        let report = request.into_inner();
        let mut state = self.state.lock().await;

        info!(
            "Task {} state: {:?}, records: {}",
            report.task_id, report.state, report.processed_records
        );
        // Update task assignment state
        state.task_assignments.remove(&report.task_id);

        Ok(Response::new(Empty {}))
    }

    async fn report_checkpoint(
        &self,
        request: Request<CheckpointReport>,
    ) -> Result<Response<Empty>, Status> {
        let report = request.into_inner();
        info!(
            "Checkpoint {} for job {} task {} success={}",
            report.checkpoint_id, report.job_id, report.task_id, report.success
        );

        Ok(Response::new(Empty {}))
    }

    async fn unregister_worker(
        &self,
        request: Request<UnregisterWorkerRequest>,
    ) -> Result<Response<Empty>, Status> {
        let req = request.into_inner();
        let mut state = self.state.lock().await;

        warn!("Worker unregistering: {}", req.worker_id);
        state.workers.remove(&req.worker_id);

        Ok(Response::new(Empty {}))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_master_state() {
        let state = MasterState::new();
        assert_eq!(state.workers.len(), 0);
        assert_eq!(state.jobs.len(), 0);
    }
}
