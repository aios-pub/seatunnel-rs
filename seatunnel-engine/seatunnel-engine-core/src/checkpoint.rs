/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.
 */

use serde::{Deserialize, Serialize};
use std::fmt;

/// Checkpoint configuration for the engine.
#[derive(Debug, Clone)]
pub struct CheckpointConfig {
    pub interval_ms: u64,
    pub max_concurrent: usize,
    pub timeout_ms: u64,
    pub min_pause_ms: u64,
    pub retention: usize,
    pub storage_backend: CheckpointStorage,
    /// Enable exactly-once semantics (disable for at-least-once).
    pub exactly_once: bool,
    /// Changelog state backend (enable for full incremental checkpoint).
    pub changelog_state_backend: bool,
}

impl Default for CheckpointConfig {
    fn default() -> Self {
        CheckpointConfig {
            interval_ms: 60_000,
            max_concurrent: 1,
            timeout_ms: 600_000,
            min_pause_ms: 30_000,
            retention: 1,
            storage_backend: CheckpointStorage::Local,
            exactly_once: true,
            changelog_state_backend: false,
        }
    }
}

/// Checkpoint storage backend selector.
#[derive(Debug, Clone, Default)]
pub enum CheckpointStorage {
    #[default]
    Local,
    HDFS(String),
    S3 {
        bucket: String,
        region: String,
    },
}

/// Unique checkpoint identifier within a job.
pub type CheckpointId = u64;

/// State snapshot captured by a single task during a checkpoint.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TaskCheckpointState {
    pub task_id: String,
    pub checkpoint_id: CheckpointId,
    pub timestamp: i64,
    pub state_data: Vec<u8>,
    pub is_done: bool,
    pub error: Option<String>,
}

impl TaskCheckpointState {
    pub fn new(task_id: String, checkpoint_id: CheckpointId, timestamp: i64) -> Self {
        TaskCheckpointState {
            task_id,
            checkpoint_id,
            timestamp,
            state_data: Vec::new(),
            is_done: false,
            error: None,
        }
    }

    pub fn complete(mut self) -> Self {
        self.is_done = true;
        self
    }

    pub fn fail(mut self, error: String) -> Self {
        self.is_done = true;
        self.error = Some(error);
        self
    }
}

/// Completed checkpoint aggregate state for a job.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CompletedCheckpoint {
    pub checkpoint_id: CheckpointId,
    pub timestamp: i64,
    pub task_states: Vec<TaskCheckpointState>,
    pub is_global: bool,
    pub is_savepoint: bool,
    pub savepoint_path: Option<String>,
}

impl CompletedCheckpoint {
    pub fn new(checkpoint_id: CheckpointId, timestamp: i64) -> Self {
        CompletedCheckpoint {
            checkpoint_id,
            timestamp,
            task_states: Vec::new(),
            is_global: false,
            is_savepoint: false,
            savepoint_path: None,
        }
    }

    pub fn add_task_state(&mut self, state: TaskCheckpointState) {
        self.task_states.push(state);
    }

    pub fn is_success(&self) -> bool {
        self.task_states
            .iter()
            .all(|s| s.is_done && s.error.is_none())
    }

    pub fn num_tasks(&self) -> usize {
        self.task_states.len()
    }

    pub fn set_as_savepoint(&mut self, path: String) {
        self.is_savepoint = true;
        self.savepoint_path = Some(path);
    }
}

/// Checkpoint lifecycle states.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CheckpointState {
    Pending,
    InProgress,
    Completed,
    Failed { reason: String },
    Cancelled,
}

impl fmt::Display for CheckpointState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            CheckpointState::Pending => write!(f, "PENDING"),
            CheckpointState::InProgress => write!(f, "IN_PROGRESS"),
            CheckpointState::Completed => write!(f, "COMPLETED"),
            CheckpointState::Failed { reason } => write!(f, "FAILED({})", reason),
            CheckpointState::Cancelled => write!(f, "CANCELLED"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_checkpoint_config_default() {
        let cfg = CheckpointConfig::default();
        assert_eq!(cfg.interval_ms, 60_000);
        assert!(cfg.exactly_once);
    }

    #[test]
    fn test_completed_checkpoint() {
        let mut cp = CompletedCheckpoint::new(1, 1000);
        let state1 = TaskCheckpointState::new("t1".to_string(), 1, 1000).complete();
        let state2 = TaskCheckpointState::new("t2".to_string(), 1, 1000).complete();
        cp.add_task_state(state1);
        cp.add_task_state(state2);
        assert!(cp.is_success());
        assert_eq!(cp.num_tasks(), 2);

        cp.set_as_savepoint("/tmp/savepoint-1".to_string());
        assert!(cp.is_savepoint);
    }

    #[test]
    fn test_failed_checkpoint() {
        let mut cp = CompletedCheckpoint::new(2, 2000);
        cp.add_task_state(
            TaskCheckpointState::new("t1".to_string(), 2, 2000).fail("OOM".to_string()),
        );
        assert!(!cp.is_success());
    }

    #[test]
    fn test_task_checkpoint_state() {
        let state = TaskCheckpointState::new("t1".to_string(), 1, 1000);
        assert!(!state.is_done);

        let completed = state.complete();
        assert!(completed.is_done);
        assert!(completed.error.is_none());
    }
}
