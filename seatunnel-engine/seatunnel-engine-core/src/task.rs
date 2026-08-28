/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.
 */

use crate::state::TaskState;

/// Unique task identifier.
pub type TaskId = String;

/// The kind of task (source reader, transform processor, sink writer).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TaskKind {
    Source,
    Transform,
    Sink,
}

/// Task status tracked by the engine.
#[derive(Debug, Clone)]
pub struct TaskStatus {
    pub task_id: TaskId,
    pub state: TaskState,
    pub processed_records: u64,
    pub start_time: i64,
    pub end_time: i64,
    pub error: Option<String>,
    /// Epoch-ms timestamp of the most recently processed record; 0 when
    /// no record has been processed yet. Lets consumers derive the task's
    /// "idle" time, which is the key liveness signal for streaming jobs.
    pub last_record_at: i64,
    /// Id and state size of the most recent completed checkpoint
    /// (id 0 = none yet). Surfaced through heartbeats so the console can
    /// show checkpoint progress for every storage backend, not only the
    /// master-backed store.
    pub last_checkpoint_id: u64,
    pub last_checkpoint_size: u64,
}

impl TaskStatus {
    pub fn new(task_id: TaskId) -> Self {
        TaskStatus {
            task_id,
            state: TaskState::Created,
            processed_records: 0,
            start_time: 0,
            end_time: 0,
            error: None,
            last_record_at: 0,
            last_checkpoint_id: 0,
            last_checkpoint_size: 0,
        }
    }

    pub fn is_running(&self) -> bool {
        matches!(self.state, TaskState::Running)
    }

    pub fn is_terminal(&self) -> bool {
        matches!(
            self.state,
            TaskState::Completed | TaskState::Failed { .. } | TaskState::Cancelled
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_task_status() {
        let mut status = TaskStatus::new("task-1".to_string());
        assert_eq!(status.state, TaskState::Created);
        assert!(!status.is_terminal());

        status.state = TaskState::Running;
        assert!(status.is_running());
        assert!(!status.is_terminal());

        status.state = TaskState::Completed;
        assert!(status.is_terminal());
    }
}
