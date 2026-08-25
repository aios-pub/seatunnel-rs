/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.
 */

/// Savepoint management for manual checkpoint and job restart.
use crate::checkpoint::{CheckpointId, CompletedCheckpoint, TaskCheckpointState};
use std::collections::HashMap;

/// Savepoint metadata.
#[derive(Debug, Clone)]
pub struct Savepoint {
    pub id: CheckpointId,
    pub path: String,
    pub job_id: String,
    pub job_name: String,
    pub timestamp: i64,
    pub task_states: Vec<TaskCheckpointState>,
    pub metadata: HashMap<String, String>,
}

impl Savepoint {
    pub fn new(id: CheckpointId, path: String, job_id: String, job_name: String) -> Self {
        Savepoint {
            id,
            path,
            job_id,
            job_name,
            timestamp: 0,
            task_states: Vec::new(),
            metadata: HashMap::new(),
        }
    }

    pub fn add_task_state(&mut self, state: TaskCheckpointState) {
        self.task_states.push(state);
    }

    pub fn with_metadata(mut self, key: &str, value: &str) -> Self {
        self.metadata.insert(key.to_string(), value.to_string());
        self
    }

    /// Create from a completed checkpoint.
    pub fn from_checkpoint(cp: &CompletedCheckpoint, path: String) -> Self {
        let mut savepoint = Savepoint {
            id: cp.checkpoint_id,
            path,
            job_id: String::new(),
            job_name: String::new(),
            timestamp: cp.timestamp,
            task_states: cp.task_states.clone(),
            metadata: HashMap::new(),
        };
        savepoint.metadata.insert(
            "origin".to_string(),
            if cp.is_savepoint {
                "manual".to_string()
            } else {
                "automatic".to_string()
            },
        );
        savepoint
    }
}

/// Savepoint manager for job lifecycle.
pub struct SavepointManager {
    savepoints: HashMap<String, Vec<Savepoint>>,
}

impl SavepointManager {
    pub fn new() -> Self {
        SavepointManager {
            savepoints: HashMap::new(),
        }
    }

    /// Create a savepoint for a job.
    pub fn create_savepoint(
        &mut self,
        job_id: &str,
        checkpoint: &CompletedCheckpoint,
        path: &str,
    ) -> Savepoint {
        let savepoint = Savepoint::from_checkpoint(checkpoint, path.to_string());
        self.savepoints
            .entry(job_id.to_string())
            .or_default()
            .push(savepoint.clone());
        savepoint
    }

    /// List all savepoints for a job.
    pub fn list_savepoints(&self, job_id: &str) -> Vec<&Savepoint> {
        self.savepoints
            .get(job_id)
            .map(|v| v.iter().collect())
            .unwrap_or_default()
    }

    /// Get the latest savepoint for a job.
    pub fn latest_savepoint(&self, job_id: &str) -> Option<&Savepoint> {
        self.savepoints.get(job_id).and_then(|v| v.last())
    }

    /// Delete a savepoint.
    pub fn delete_savepoint(&mut self, job_id: &str, id: CheckpointId) -> bool {
        if let Some(savepoints) = self.savepoints.get_mut(job_id) {
            let len_before = savepoints.len();
            savepoints.retain(|s| s.id != id);
            savepoints.len() < len_before
        } else {
            false
        }
    }

    /// Get the total savepoint count across all jobs.
    pub fn total_count(&self) -> usize {
        self.savepoints.values().map(|v| v.len()).sum()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::checkpoint::CompletedCheckpoint;

    #[test]
    fn test_savepoint_creation() {
        let mut cp = CompletedCheckpoint::new(42, 5000);
        cp.add_task_state(TaskCheckpointState::new("t1".to_string(), 42, 5000).complete());

        let mut manager = SavepointManager::new();
        let sp = manager.create_savepoint("job-1", &cp, "/tmp/sp-1");
        assert_eq!(sp.id, 42);
        assert_eq!(sp.path, "/tmp/sp-1");
        assert_eq!(sp.task_states.len(), 1);
    }

    #[test]
    fn test_savepoint_list_and_latest() {
        let mut manager = SavepointManager::new();

        let cp1 = CompletedCheckpoint::new(1, 1000);
        manager.create_savepoint("job-1", &cp1, "/tmp/sp-1");

        let cp2 = CompletedCheckpoint::new(2, 2000);
        manager.create_savepoint("job-1", &cp2, "/tmp/sp-2");

        let cp3 = CompletedCheckpoint::new(3, 3000);
        manager.create_savepoint("job-2", &cp3, "/tmp/sp-3");

        assert_eq!(manager.list_savepoints("job-1").len(), 2);
        assert_eq!(manager.latest_savepoint("job-1").unwrap().id, 2);
        assert_eq!(manager.total_count(), 3);
    }

    #[test]
    fn test_delete_savepoint() {
        let mut manager = SavepointManager::new();

        let cp = CompletedCheckpoint::new(1, 1000);
        manager.create_savepoint("job-1", &cp, "/tmp/sp-1");

        assert!(manager.delete_savepoint("job-1", 1));
        assert!(manager.list_savepoints("job-1").is_empty());
        assert!(!manager.delete_savepoint("job-1", 1));
    }

    #[test]
    fn test_savepoint_with_metadata() {
        let mut cp = CompletedCheckpoint::new(10, 10000);
        cp.add_task_state(TaskCheckpointState::new("t1".to_string(), 10, 10000).complete());

        let mut manager = SavepointManager::new();
        let sp = manager.create_savepoint("job-1", &cp, "/tmp/sp");
        assert_eq!(sp.task_states.len(), 1);
    }
}
