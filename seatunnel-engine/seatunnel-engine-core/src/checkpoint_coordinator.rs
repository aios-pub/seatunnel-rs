/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.
 */

use std::collections::HashMap;
use std::fmt;
use tokio::sync::mpsc;

use crate::barrier::CheckpointBarrier;
use crate::checkpoint::{CheckpointConfig, CheckpointId, CompletedCheckpoint, TaskCheckpointState};
use crate::checkpoint_storage::{CheckpointStorageBackend, InMemoryCheckpointStorage};
use crate::recovery::{FailureEvent, RecoveryManager};

/// The CheckpointCoordinator is the central authority for exactly-once checkpointing.
///
/// Responsibilities:
/// 1. Trigger checkpoints periodically (or on demand)
/// 2. Send barriers to all running tasks
/// 3. Collect checkpoint acknowledgements from tasks
/// 4. Aggregate checkpoint results
/// 5. Store completed checkpoints to the configured backend
/// 6. Handle failures via the RecoveryManager
///
/// ```text
/// Timer ---triggers--> CheckpointCoordinator
///                            |
///                    sends barriers
///                            |
///                    +-------+-------+
///                    v       v       v
///                  Task1   Task2   Task3
///                    |       |       |
///                    +-------+-------+
///                            |
///                    collect acks
///                            |
///                    +-------+-------+
///                    |               |
///              if all acks   if any fail
///                    |               |
///                    v               v
///             store checkpoint   fail + recover
/// ```
pub struct CheckpointCoordinator {
    job_id: String,
    config: CheckpointConfig,
    storage: Box<dyn CheckpointStorageBackend>,
    recovery_manager: RecoveryManager,

    /// Next checkpoint id to assign.
    next_checkpoint_id: CheckpointId,

    /// Running tasks that need to participate in checkpoints.
    task_ids: Vec<String>,

    /// Pending checkpoint: id → partial results.
    pending_checkpoint: Option<(CheckpointId, HashMap<String, TaskCheckpointState>)>,

    /// Completed checkpoints (kept in memory for recovery).
    completed_checkpoints: HashMap<CheckpointId, CompletedCheckpoint>,

    /// Channel to receive checkpoint reports from tasks.
    #[allow(dead_code)] // reports are currently consumed via report_tx clones
    report_rx: mpsc::UnboundedReceiver<(CheckpointId, TaskCheckpointState)>,
    report_tx: mpsc::UnboundedSender<(CheckpointId, TaskCheckpointState)>,
}

impl CheckpointCoordinator {
    pub fn new(job_id: String, config: CheckpointConfig) -> Self {
        let (report_tx, report_rx) = mpsc::unbounded_channel();
        CheckpointCoordinator {
            job_id,
            config,
            storage: Box::new(InMemoryCheckpointStorage::new()),
            recovery_manager: RecoveryManager::new(3),
            next_checkpoint_id: 1,
            task_ids: Vec::new(),
            pending_checkpoint: None,
            completed_checkpoints: HashMap::new(),
            report_rx,
            report_tx,
        }
    }

    /// Set a custom storage backend.
    pub fn with_storage(mut self, storage: Box<dyn CheckpointStorageBackend>) -> Self {
        self.storage = storage;
        self
    }

    /// Register a task with the coordinator.
    pub fn register_task(&mut self, task_id: String) {
        self.task_ids.push(task_id);
    }

    /// Get the report sender channel for tasks to submit checkpoint results.
    pub fn report_channel(&self) -> mpsc::UnboundedSender<(CheckpointId, TaskCheckpointState)> {
        self.report_tx.clone()
    }

    /// Trigger a new checkpoint.
    pub fn trigger_checkpoint(&mut self, _timestamp: i64) -> Option<CheckpointId> {
        if let Some((_, _)) = &self.pending_checkpoint {
            return None;
        }

        let cp_id = self.next_checkpoint_id;
        self.next_checkpoint_id += 1;

        self.pending_checkpoint = Some((cp_id, HashMap::new()));
        Some(cp_id)
    }

    /// Check if there is a pending checkpoint.
    pub fn has_pending(&self) -> bool {
        self.pending_checkpoint.is_some()
    }

    /// Build the checkpoint barrier for the current pending checkpoint.
    pub fn get_pending_barrier(&self) -> Option<CheckpointBarrier> {
        self.pending_checkpoint.as_ref().map(|(cp_id, _)| {
            let now = crate::now_millis();
            CheckpointBarrier::new(*cp_id, now)
        })
    }

    /// Process a checkpoint report from a task.
    /// Returns Some(CompletedCheckpoint) if the checkpoint is complete.
    pub fn process_report(
        &mut self,
        checkpoint_id: CheckpointId,
        task_state: TaskCheckpointState,
    ) -> Option<CompletedCheckpoint> {
        // Check if there's a pending checkpoint matching this id
        let pending_id = match &self.pending_checkpoint {
            Some((id, _)) if *id == checkpoint_id => *id,
            _ => return None,
        };

        // Insert the task state
        if let Some((_, ref mut pending)) = self.pending_checkpoint {
            pending.insert(task_state.task_id.clone(), task_state);
        }

        // Check if all tasks have reported
        let pending = self.pending_checkpoint.as_ref().map(|(_, m)| m);
        let all_done = if let Some(p) = pending {
            self.task_ids.iter().all(|tid| p.contains_key(tid))
        } else {
            false
        };

        if !all_done {
            return None;
        }

        // Take ownership of the pending states
        let pending_states: HashMap<String, TaskCheckpointState> = self
            .pending_checkpoint
            .take()
            .map(|(_, m)| m)
            .unwrap_or_default();
        self.pending_checkpoint = None;

        let timestamp = crate::now_millis();
        let mut completed = CompletedCheckpoint::new(pending_id, timestamp);
        for (_, state) in pending_states {
            completed.add_task_state(state);
        }

        if completed.is_success() {
            if self.storage.store(&completed).is_ok() {
                self.completed_checkpoints
                    .insert(pending_id, completed.clone());
                self.recovery_manager
                    .record_checkpoint(&self.job_id, &completed);
                self.enforce_retention();
                return Some(completed);
            }
        } else {
            let reason = "one or more tasks failed checkpoint".to_string();
            self.recovery_manager.handle_failure(
                &self.job_id,
                &FailureEvent::TaskFailed {
                    task_id: "unknown".to_string(),
                    error: reason,
                },
            );
        }

        Some(completed)
    }

    /// Enforce retention policy - delete old checkpoints.
    fn enforce_retention(&mut self) {
        let retention = self.config.retention;
        if self.completed_checkpoints.len() > retention {
            let mut ids: Vec<CheckpointId> = self.completed_checkpoints.keys().cloned().collect();
            ids.sort();
            let to_remove = ids.len() - retention;
            for id in ids[..to_remove].iter() {
                let _ = self.storage.delete(*id);
                self.completed_checkpoints.remove(id);
            }
        }
    }

    /// Get the number of completed checkpoints.
    pub fn completed_count(&self) -> usize {
        self.completed_checkpoints.len()
    }

    /// Get the last completed checkpoint.
    pub fn last_completed(&self) -> Option<&CompletedCheckpoint> {
        self.completed_checkpoints
            .iter()
            .max_by_key(|(&id, _)| id)
            .map(|(_, v)| v)
    }

    /// Get the next checkpoint id that will be assigned.
    pub fn next_checkpoint_id(&self) -> CheckpointId {
        self.next_checkpoint_id
    }

    /// Handle a failure and produce a recovery plan.
    pub fn handle_failure(&mut self, event: FailureEvent) -> Option<crate::recovery::RecoveryPlan> {
        self.recovery_manager.handle_failure(&self.job_id, &event)
    }

    /// Get the recovery manager for accessing recovery state.
    pub fn recovery_manager(&self) -> &RecoveryManager {
        &self.recovery_manager
    }

    /// Get the checkpoint config.
    pub fn config(&self) -> &CheckpointConfig {
        &self.config
    }

    /// Get the number of registered tasks.
    pub fn task_count(&self) -> usize {
        self.task_ids.len()
    }
}

impl fmt::Display for CheckpointCoordinator {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "CheckpointCoordinator(job={}, tasks={}, next_cp={}, completed={})",
            self.job_id,
            self.task_count(),
            self.next_checkpoint_id(),
            self.completed_count()
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_trigger_checkpoint() {
        let mut coord =
            CheckpointCoordinator::new("job-1".to_string(), CheckpointConfig::default());
        coord.register_task("t1".to_string());
        coord.register_task("t2".to_string());
        coord.register_task("t3".to_string());

        let cp_id = coord.trigger_checkpoint(1000);
        assert_eq!(cp_id, Some(1));
        assert!(coord.has_pending());
        assert_eq!(coord.next_checkpoint_id(), 2);

        // Should not trigger another while one is pending
        assert_eq!(coord.trigger_checkpoint(2000), None);
    }

    #[test]
    fn test_complete_checkpoint() {
        let mut coord =
            CheckpointCoordinator::new("job-1".to_string(), CheckpointConfig::default());
        coord.register_task("t1".to_string());
        coord.register_task("t2".to_string());

        coord.trigger_checkpoint(1000);

        // Report from t1
        let state1 = TaskCheckpointState::new("t1".to_string(), 1, 1000).complete();
        assert!(coord.process_report(1, state1).is_none());

        // Report from t2 - should complete
        let state2 = TaskCheckpointState::new("t2".to_string(), 1, 1000).complete();
        let completed = coord.process_report(1, state2);

        assert!(completed.is_some());
        let completed = completed.unwrap();
        assert!(completed.is_success());
        assert_eq!(completed.num_tasks(), 2);
        assert_eq!(coord.completed_count(), 1);
        assert!(!coord.has_pending());
    }

    #[test]
    fn test_failed_checkpoint() {
        let mut coord =
            CheckpointCoordinator::new("job-1".to_string(), CheckpointConfig::default());
        coord.register_task("t1".to_string());
        coord.register_task("t2".to_string());

        coord.trigger_checkpoint(1000);

        let state1 = TaskCheckpointState::new("t1".to_string(), 1, 1000).complete();
        coord.process_report(1, state1);

        let state2 = TaskCheckpointState::new("t2".to_string(), 1, 1000).fail("OOM".to_string());
        let completed = coord.process_report(1, state2).unwrap();

        assert!(!completed.is_success());
        assert_eq!(coord.completed_count(), 0);
    }

    #[test]
    fn test_retention_policy() {
        let cfg = CheckpointConfig {
            retention: 2,
            ..Default::default()
        };
        let mut coord = CheckpointCoordinator::new("job-1".to_string(), cfg);
        coord.register_task("t1".to_string());

        // Create 5 checkpoints
        for i in 1u64..=5 {
            coord.trigger_checkpoint((i * 1000) as i64);
            let state = TaskCheckpointState::new("t1".to_string(), i, (i * 1000) as i64).complete();
            coord.process_report(i, state);
        }

        assert_eq!(coord.completed_count(), 2); // retention=2
    }

    #[test]
    fn test_failure_recovery() {
        let mut coord =
            CheckpointCoordinator::new("job-1".to_string(), CheckpointConfig::default());

        let event = FailureEvent::TaskFailed {
            task_id: "t1".to_string(),
            error: "crash".to_string(),
        };
        let plan = coord.handle_failure(event);
        assert!(plan.is_some());
        let plan = plan.unwrap();
        assert_eq!(plan.failed_tasks, vec!["t1"]);
    }

    #[test]
    fn test_display() {
        let coord = CheckpointCoordinator::new("job-1".to_string(), CheckpointConfig::default());
        assert!(coord.to_string().contains("job-1"));
        assert!(coord.to_string().contains("next_cp=1"));
    }
}
