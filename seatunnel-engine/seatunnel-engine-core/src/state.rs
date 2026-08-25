/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.
 */

use std::fmt;

/// Job execution state machine.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum JobState {
    Created,
    Scheduled,
    Running,
    Completed,
    Failed { error: String },
    Cancelled,
}

impl fmt::Display for JobState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            JobState::Created => write!(f, "CREATED"),
            JobState::Scheduled => write!(f, "SCHEDULED"),
            JobState::Running => write!(f, "RUNNING"),
            JobState::Completed => write!(f, "COMPLETED"),
            JobState::Failed { error } => write!(f, "FAILED({})", error),
            JobState::Cancelled => write!(f, "CANCELLED"),
        }
    }
}

/// Task execution state machine.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TaskState {
    Created,
    Running,
    Completed,
    Failed { error: String },
    Cancelled,
}

impl fmt::Display for TaskState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            TaskState::Created => write!(f, "CREATED"),
            TaskState::Running => write!(f, "RUNNING"),
            TaskState::Completed => write!(f, "COMPLETED"),
            TaskState::Failed { error } => write!(f, "FAILED({})", error),
            TaskState::Cancelled => write!(f, "CANCELLED"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_job_state_display() {
        assert_eq!(JobState::Running.to_string(), "RUNNING");
        assert_eq!(
            JobState::Failed {
                error: "OOM".to_string()
            }
            .to_string(),
            "FAILED(OOM)"
        );
    }

    #[test]
    fn test_task_state_display() {
        assert_eq!(TaskState::Completed.to_string(), "COMPLETED");
    }
}
