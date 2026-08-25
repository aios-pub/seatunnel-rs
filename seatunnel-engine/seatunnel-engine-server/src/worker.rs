/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.
 */

//! Worker node implementation.
//!
//! A Worker executes tasks assigned by the Master. It maintains a heartbeat
//! connection and reports task status updates.

use seatunnel_engine_comm::HeartbeatRequest;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{mpsc, Mutex};
use tokio::time::interval;
use tracing::{error, info, warn};

use crate::resource_manager::ResourceManager;

/// Worker node state.
pub struct WorkerNode {
    worker_id: String,
    address: String,
    master_address: String,
    resource_manager: Arc<ResourceManager>,
    running_tasks: Mutex<HashMap<String, TaskExecutionInfo>>,
    heartbeat_task: Option<tokio::task::JoinHandle<()>>,
}

#[derive(Debug, Clone)]
pub struct TaskExecutionInfo {
    pub task_id: String,
    pub job_id: String,
    pub stage_id: String,
    pub state: String,
    pub processed_records: u64,
    pub start_time: i64,
}

impl WorkerNode {
    pub fn new(worker_id: String, address: String, master_address: String) -> Self {
        WorkerNode {
            worker_id,
            address,
            master_address,
            resource_manager: Arc::new(ResourceManager::new()),
            running_tasks: Mutex::new(HashMap::new()),
            heartbeat_task: None,
        }
    }

    /// Register this worker with the resource manager.
    pub fn register(&self, cpu_cores: u32, memory_bytes: u64) {
        self.resource_manager.register_worker(
            self.worker_id.clone(),
            self.address.clone(),
            cpu_cores,
            memory_bytes,
        );
    }

    /// Start the heartbeat loop (in-memory, without gRPC).
    pub fn start_heartbeat_loop(
        &mut self,
        interval_ms: u64,
        tx: mpsc::UnboundedSender<HeartbeatRequest>,
    ) {
        let worker_id = self.worker_id.clone();
        let address = self.address.clone();
        let mut interval_handle = interval(Duration::from_millis(interval_ms));

        let handle = tokio::spawn(async move {
            loop {
                interval_handle.tick().await;
                let hb = HeartbeatRequest {
                    worker_id: worker_id.clone(),
                    address: address.clone(),
                    timestamp: 0,
                    tasks: vec![],
                };
                if tx.send(hb).is_err() {
                    warn!("Heartbeat channel closed");
                    break;
                }
            }
        });

        self.heartbeat_task = Some(handle);
    }

    /// Stop the heartbeat loop.
    pub fn stop_heartbeat(&mut self) {
        if let Some(handle) = self.heartbeat_task.take() {
            handle.abort();
        }
    }

    /// Assign a task for execution.
    pub async fn assign_task(&self, task_id: String, job_id: String, stage_id: String) {
        let task_id_clone = task_id.clone();
        let mut tasks = self.running_tasks.lock().await;
        tasks.insert(
            task_id,
            TaskExecutionInfo {
                task_id: task_id_clone.clone(),
                job_id,
                stage_id,
                state: "CREATED".to_string(),
                processed_records: 0,
                start_time: 0,
            },
        );
        drop(tasks);
        info!(
            "Assigned task {} to worker {}",
            task_id_clone, self.worker_id
        );
    }

    /// Update task status.
    pub async fn update_task_state(&self, task_id: &str, state: String) {
        let mut tasks = self.running_tasks.lock().await;
        if let Some(info) = tasks.get_mut(task_id) {
            info.state = state;
        }
    }

    /// Update task record count.
    pub async fn update_task_records(&self, task_id: &str, count: u64) {
        let mut tasks = self.running_tasks.lock().await;
        if let Some(info) = tasks.get_mut(task_id) {
            info.processed_records = count;
        }
    }

    /// Get all running tasks.
    pub async fn running_task_count(&self) -> usize {
        let tasks = self.running_tasks.lock().await;
        tasks.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_worker_assign_task() {
        let mut worker = WorkerNode::new(
            "w1".to_string(),
            "127.0.0.1:5001".to_string(),
            "127.0.0.1:5000".to_string(),
        );
        worker.register(8, 8 * 1024 * 1024 * 1024);

        worker
            .assign_task(
                "task-1".to_string(),
                "job-1".to_string(),
                "stage-1".to_string(),
            )
            .await;
        assert_eq!(worker.running_task_count().await, 1);

        worker
            .update_task_state("task-1", "RUNNING".to_string())
            .await;
        worker.update_task_records("task-1", 100).await;
    }
}
