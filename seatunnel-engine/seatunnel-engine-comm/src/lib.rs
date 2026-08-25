/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.
 */

//! Engine Communication: gRPC service layer for Master/Worker/Client communication.
//!
//! Generated from `src/proto/master.proto` via tonic-build.

pub mod generated {
    tonic::include_proto!("seatunnel.engine.v1");
}

use async_trait::async_trait;
use std::collections::HashMap;
use tokio::sync::mpsc;
use tracing::{error, info, warn};

pub use generated::client_service_client::ClientServiceClient;
pub use generated::client_service_server::ClientService;
pub use generated::master_service_client::MasterServiceClient;
pub use generated::master_service_server::MasterService;
pub use generated::{
    CancelJobRequest, CheckpointReport, ClusterInfo, Empty, HeartbeatRequest, HeartbeatResponse,
    JobList, JobStatus, JobStatusRequest, JobSummary, SubmitJobRequest, SubmitJobResponse,
    TaskDescriptor, TaskHeartbeat, TaskStatusInfo, TaskStatusReport, UnregisterWorkerRequest,
    WorkerInfo, WorkerRegistration, WorkerRegistrationResponse,
};

use generated::{JobState as ProtoJobState, TaskState as ProtoTaskState};

use seatunnel_engine_core::state::{JobState as CoreJobState, TaskState as CoreTaskState};
use seatunnel_engine_core::{JobId, TaskId};

/// Converts a core TaskState to the protobuf TaskState.
pub fn task_state_to_proto(state: &CoreTaskState) -> i32 {
    match state {
        CoreTaskState::Created => ProtoTaskState::TaskCreated as i32,
        CoreTaskState::Running => ProtoTaskState::TaskRunning as i32,
        CoreTaskState::Completed => ProtoTaskState::TaskCompleted as i32,
        CoreTaskState::Failed { .. } => ProtoTaskState::TaskFailed as i32,
        CoreTaskState::Cancelled => ProtoTaskState::TaskCancelled as i32,
    }
}

/// Converts a protobuf TaskState to a core TaskState.
pub fn task_state_from_proto(proto: i32) -> CoreTaskState {
    match proto {
        x if x == ProtoTaskState::TaskRunning as i32 => CoreTaskState::Running,
        x if x == ProtoTaskState::TaskCompleted as i32 => CoreTaskState::Completed,
        x if x == ProtoTaskState::TaskFailed as i32 => CoreTaskState::Failed {
            error: "unknown".to_string(),
        },
        x if x == ProtoTaskState::TaskCancelled as i32 => CoreTaskState::Cancelled,
        _ => CoreTaskState::Created,
    }
}

/// Converts a core JobState to the protobuf JobState.
pub fn job_state_to_proto(state: &CoreJobState) -> i32 {
    match state {
        CoreJobState::Created => ProtoJobState::JobCreated as i32,
        CoreJobState::Scheduled => ProtoJobState::JobScheduled as i32,
        CoreJobState::Running => ProtoJobState::JobRunning as i32,
        CoreJobState::Completed => ProtoJobState::JobCompleted as i32,
        CoreJobState::Failed { .. } => ProtoJobState::JobFailed as i32,
        CoreJobState::Cancelled => ProtoJobState::JobCancelled as i32,
    }
}

/// Builds a TaskDescriptor from a core Task definition.
pub fn build_task_descriptor(
    task_id: TaskId,
    job_id: JobId,
    stage_id: String,
    task_name: String,
    task_index: u32,
    config: HashMap<String, String>,
) -> TaskDescriptor {
    TaskDescriptor {
        task_id,
        job_id,
        stage_id,
        task_name,
        task_index: task_index as i32,
        source_config_json: String::new(),
        sink_config_json: String::new(),
        parallelism: 1,
        config,
    }
}

/// Heartbeat message from worker to master.
#[derive(Debug, Clone)]
pub struct WorkerHeartbeat {
    pub worker_id: String,
    pub address: String,
    pub timestamp: i64,
    pub tasks: Vec<WorkerTaskHeartbeat>,
}

#[derive(Debug, Clone)]
pub struct WorkerTaskHeartbeat {
    pub task_id: String,
    pub state: CoreTaskState,
    pub processed_records: i64,
    pub last_heartbeat_time: i64,
    pub memory_usage: i64,
}

impl WorkerHeartbeat {
    pub fn to_proto(&self) -> HeartbeatRequest {
        HeartbeatRequest {
            worker_id: self.worker_id.clone(),
            address: self.address.clone(),
            timestamp: self.timestamp,
            tasks: self
                .tasks
                .iter()
                .map(|t| TaskHeartbeat {
                    task_id: t.task_id.clone(),
                    state: task_state_to_proto(&t.state),
                    processed_records: t.processed_records,
                    last_heartbeat_time: t.last_heartbeat_time,
                    memory_usage: t.memory_usage,
                })
                .collect(),
        }
    }
}

/// Trait for the job scheduler that decides task placement.
#[async_trait]
pub trait Scheduler: Send + Sync {
    async fn schedule_tasks(&self, job_id: &str, task_count: usize) -> Vec<TaskAssignment>;
    async fn reschedule_task(&self, task_id: &str) -> Option<TaskAssignment>;
}

#[derive(Debug, Clone)]
pub struct TaskAssignment {
    pub task_id: String,
    pub worker_id: String,
    pub worker_address: String,
}

/// In-memory scheduler implementation.
pub struct InMemoryScheduler {
    /// Available workers in round-robin order.
    workers: Vec<(String, String)>,
}

impl InMemoryScheduler {
    pub fn new(workers: Vec<(String, String)>) -> Self {
        InMemoryScheduler { workers }
    }

    pub fn set_workers(&mut self, workers: Vec<(String, String)>) {
        self.workers = workers;
    }
}

#[async_trait]
impl Scheduler for InMemoryScheduler {
    async fn schedule_tasks(&self, _job_id: &str, task_count: usize) -> Vec<TaskAssignment> {
        if self.workers.is_empty() {
            return vec![];
        }
        (0..task_count)
            .map(|i| {
                let (worker_id, address) = &self.workers[i % self.workers.len()];
                TaskAssignment {
                    task_id: format!("task-{}", i),
                    worker_id: worker_id.clone(),
                    worker_address: address.clone(),
                }
            })
            .collect()
    }

    async fn reschedule_task(&self, task_id: &str) -> Option<TaskAssignment> {
        if self.workers.is_empty() {
            return None;
        }
        Some(TaskAssignment {
            task_id: task_id.to_string(),
            worker_id: self.workers[0].0.clone(),
            worker_address: self.workers[0].1.clone(),
        })
    }
}

/// Asynchronous message channel for inter-component communication.
pub type TaskChannel = mpsc::UnboundedSender<TaskDescriptor>;
pub type TaskReceiver = mpsc::UnboundedReceiver<TaskDescriptor>;

pub fn create_task_channel() -> (TaskChannel, TaskReceiver) {
    mpsc::unbounded_channel()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_task_state_conversion() {
        assert_eq!(
            task_state_to_proto(&CoreTaskState::Running),
            ProtoTaskState::TaskRunning as i32
        );
        assert_eq!(
            task_state_from_proto(ProtoTaskState::TaskRunning as i32),
            CoreTaskState::Running
        );
    }

    #[test]
    fn test_job_state_conversion() {
        assert_eq!(
            job_state_to_proto(&CoreJobState::Running),
            ProtoJobState::JobRunning as i32
        );
    }

    #[tokio::test]
    async fn test_scheduler() {
        let workers = vec![
            ("w1".to_string(), "127.0.0.1:5001".to_string()),
            ("w2".to_string(), "127.0.0.1:5002".to_string()),
        ];
        let scheduler = InMemoryScheduler::new(workers);
        let assignments = scheduler.schedule_tasks("job-1", 4).await;
        assert_eq!(assignments.len(), 4);
        assert_eq!(assignments[0].worker_id, "w1");
        assert_eq!(assignments[1].worker_id, "w2");
        assert_eq!(assignments[2].worker_id, "w1");
    }
}
