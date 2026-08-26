/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.
 */

use std::collections::HashMap;
use std::fmt;

use crate::checkpoint::{CheckpointId, CompletedCheckpoint};

/// Failure event that triggers recovery.
#[derive(Debug, Clone)]
pub enum FailureEvent {
    /// A task crashed or reported an error.
    TaskFailed { task_id: String, error: String },
    /// A worker node stopped responding.
    WorkerLost {
        worker_id: String,
        tasks: Vec<String>,
    },
    /// The master itself failed (requires leader re-election).
    MasterFailed,
}

impl fmt::Display for FailureEvent {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            FailureEvent::TaskFailed { task_id, error } => {
                write!(f, "Task {} failed: {}", task_id, error)
            }
            FailureEvent::WorkerLost { worker_id, tasks } => {
                write!(f, "Worker {} lost ({} tasks)", worker_id, tasks.len())
            }
            FailureEvent::MasterFailed => write!(f, "Master failed"),
        }
    }
}

/// Recovery plan for handling a failure.
#[derive(Debug, Clone)]
pub struct RecoveryPlan {
    pub failed_tasks: Vec<String>,
    pub last_checkpoint_id: Option<CheckpointId>,
    pub restart_from_checkpoint: bool,
    pub recovery_action: RecoveryAction,
}

#[derive(Debug, Clone)]
pub enum RecoveryAction {
    /// Restart the failed task(s) from the last checkpoint.
    RestartFromCheckpoint { checkpoint_id: CheckpointId },
    /// Restart from the beginning (no checkpoint available).
    RestartFromBeginning,
    /// The entire job needs to be restarted.
    RestartJob,
    /// No action needed (transient failure).
    Retry,
}

impl fmt::Display for RecoveryAction {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            RecoveryAction::RestartFromCheckpoint { checkpoint_id } => {
                write!(f, "RestartFromCheckpoint({})", checkpoint_id)
            }
            RecoveryAction::RestartFromBeginning => write!(f, "RestartFromBeginning"),
            RecoveryAction::RestartJob => write!(f, "RestartJob"),
            RecoveryAction::Retry => write!(f, "Retry"),
        }
    }
}

/// Task recovery state.
#[derive(Debug, Clone)]
pub enum RecoveryState {
    /// Task is being recovered.
    Recovering { checkpoint_id: CheckpointId },
    /// Task has been recovered and is running.
    Recovered,
    /// Recovery failed.
    RecoveryFailed { error: String },
}

impl fmt::Display for RecoveryState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            RecoveryState::Recovering { checkpoint_id } => {
                write!(f, "Recovering from CP#{}", checkpoint_id)
            }
            RecoveryState::Recovered => write!(f, "Recovered"),
            RecoveryState::RecoveryFailed { error } => write!(f, "RecoveryFailed({})", error),
        }
    }
}

/// Failure recovery manager.
pub struct RecoveryManager {
    /// Last known good checkpoint for each job.
    last_checkpoints: HashMap<String, CompletedCheckpoint>,
    /// Current recovery state for each task.
    recovery_states: HashMap<String, RecoveryState>,
    /// Maximum recovery attempts before giving up.
    max_retries: u32,
    /// Current retry count per task.
    retry_counts: HashMap<String, u32>,
}

impl RecoveryManager {
    pub fn new(max_retries: u32) -> Self {
        RecoveryManager {
            last_checkpoints: HashMap::new(),
            recovery_states: HashMap::new(),
            max_retries,
            retry_counts: HashMap::new(),
        }
    }

    /// Record a successful checkpoint for a job.
    pub fn record_checkpoint(&mut self, job_id: &str, checkpoint: &CompletedCheckpoint) {
        self.last_checkpoints
            .insert(job_id.to_string(), checkpoint.clone());
    }

    /// Handle a failure event and produce a recovery plan.
    pub fn handle_failure(&mut self, job_id: &str, event: &FailureEvent) -> Option<RecoveryPlan> {
        let (failed_tasks, restart_from_checkpoint, _last_cp_id) = match event {
            FailureEvent::TaskFailed { task_id, .. } => {
                (vec![task_id.clone()], true, None as Option<CheckpointId>)
            }
            FailureEvent::WorkerLost {
                worker_id: _,
                tasks,
            } => (tasks.clone(), true, None as Option<CheckpointId>),
            FailureEvent::MasterFailed => {
                return Some(RecoveryPlan {
                    failed_tasks: vec![],
                    last_checkpoint_id: None,
                    restart_from_checkpoint: false,
                    recovery_action: RecoveryAction::RestartJob,
                });
            }
        };

        // Find the last successful checkpoint
        let last_cp_id = self.last_checkpoints.get(job_id).map(|cp| cp.checkpoint_id);

        let recovery_action = if let Some(cp_id) = last_cp_id {
            if let Some(retry_count) = self.retry_counts.get(&failed_tasks[0]).copied() {
                if retry_count >= self.max_retries {
                    RecoveryAction::RestartJob
                } else {
                    RecoveryAction::RestartFromCheckpoint {
                        checkpoint_id: cp_id,
                    }
                }
            } else {
                RecoveryAction::RestartFromCheckpoint {
                    checkpoint_id: cp_id,
                }
            }
        } else {
            RecoveryAction::RestartFromBeginning
        };

        // Update retry counts
        for task in &failed_tasks {
            let count = self.retry_counts.entry(task.clone()).or_insert(0);
            *count += 1;
        }

        Some(RecoveryPlan {
            failed_tasks,
            last_checkpoint_id: last_cp_id,
            restart_from_checkpoint,
            recovery_action,
        })
    }

    /// Start recovering a task from a checkpoint.
    pub fn start_recovery(&mut self, task_id: &str, checkpoint_id: CheckpointId) {
        self.recovery_states.insert(
            task_id.to_string(),
            RecoveryState::Recovering { checkpoint_id },
        );
    }

    /// Complete recovery for a task.
    pub fn complete_recovery(&mut self, task_id: &str) {
        self.recovery_states
            .insert(task_id.to_string(), RecoveryState::Recovered);
        self.retry_counts.remove(task_id);
    }

    /// Fail recovery for a task.
    pub fn fail_recovery(&mut self, task_id: &str, error: String) {
        self.recovery_states
            .insert(task_id.to_string(), RecoveryState::RecoveryFailed { error });
    }

    /// Get recovery state for a task.
    pub fn recovery_state(&self, task_id: &str) -> Option<&RecoveryState> {
        self.recovery_states.get(task_id)
    }

    /// Get the last checkpoint for a job.
    pub fn last_checkpoint(&self, job_id: &str) -> Option<&CompletedCheckpoint> {
        self.last_checkpoints.get(job_id)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::checkpoint::TaskCheckpointState;

    #[test]
    fn test_task_failure_recovery() {
        let mut mgr = RecoveryManager::new(3);

        // Record a checkpoint
        let mut cp = CompletedCheckpoint::new(5, 5000);
        cp.add_task_state(TaskCheckpointState::new("t1".to_string(), 5, 5000).complete());
        mgr.record_checkpoint("job-1", &cp);

        // Simulate failure
        let event = FailureEvent::TaskFailed {
            task_id: "t1".to_string(),
            error: "OOM".to_string(),
        };
        let plan = mgr.handle_failure("job-1", &event).unwrap();

        assert_eq!(plan.failed_tasks, vec!["t1"]);
        assert!(plan.restart_from_checkpoint);
        match plan.recovery_action {
            RecoveryAction::RestartFromCheckpoint { checkpoint_id } => {
                assert_eq!(checkpoint_id, 5);
            }
            _ => panic!("Expected RestartFromCheckpoint"),
        }
    }

    #[test]
    fn test_no_checkpoint_restart_from_beginning() {
        let mut mgr = RecoveryManager::new(3);

        let event = FailureEvent::TaskFailed {
            task_id: "t1".to_string(),
            error: "crash".to_string(),
        };
        let plan = mgr.handle_failure("job-1", &event).unwrap();

        assert!(plan.restart_from_checkpoint);
        match plan.recovery_action {
            RecoveryAction::RestartFromBeginning => {}
            _ => panic!("Expected RestartFromBeginning"),
        }
    }

    #[test]
    fn test_max_retries_exceeded() {
        let mut mgr = RecoveryManager::new(2);

        let mut cp = CompletedCheckpoint::new(1, 1000);
        cp.add_task_state(TaskCheckpointState::new("t1".to_string(), 1, 1000).complete());
        mgr.record_checkpoint("job-1", &cp);

        // First failure
        let event = FailureEvent::TaskFailed {
            task_id: "t1".to_string(),
            error: "e1".to_string(),
        };
        let _ = mgr.handle_failure("job-1", &event);

        // Second failure (at limit)
        let _ = mgr.handle_failure("job-1", &event);

        // Third failure (exceeds limit)
        let event = FailureEvent::TaskFailed {
            task_id: "t1".to_string(),
            error: "e3".to_string(),
        };
        let plan = mgr.handle_failure("job-1", &event).unwrap();

        assert!(matches!(plan.recovery_action, RecoveryAction::RestartJob));
    }

    #[test]
    fn test_recovery_state_lifecycle() {
        let mut mgr = RecoveryManager::new(3);

        mgr.start_recovery("t1", 5);
        assert!(matches!(
            mgr.recovery_state("t1"),
            Some(RecoveryState::Recovering { .. })
        ));

        mgr.complete_recovery("t1");
        assert!(matches!(
            mgr.recovery_state("t1"),
            Some(RecoveryState::Recovered)
        ));
    }

    #[test]
    fn test_worker_lost_recovery() {
        let mut mgr = RecoveryManager::new(3);

        let mut cp = CompletedCheckpoint::new(3, 3000);
        cp.add_task_state(TaskCheckpointState::new("t1".to_string(), 3, 3000).complete());
        cp.add_task_state(TaskCheckpointState::new("t2".to_string(), 3, 3000).complete());
        mgr.record_checkpoint("job-1", &cp);

        let event = FailureEvent::WorkerLost {
            worker_id: "w1".to_string(),
            tasks: vec!["t1".to_string(), "t2".to_string()],
        };
        let plan = mgr.handle_failure("job-1", &event).unwrap();
        assert_eq!(plan.failed_tasks.len(), 2);
    }

    #[test]
    fn test_failure_event_display() {
        let event = FailureEvent::TaskFailed {
            task_id: "t1".to_string(),
            error: "OOM".to_string(),
        };
        assert_eq!(event.to_string(), "Task t1 failed: OOM");
    }
}
