/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.
 */

//! State backend abstraction for task state management.
//!
//! State backends define how task state is stored and checkpointed.
//! The engine supports memory-only state (for testing) and managed state
//! (remote storage with local caching).

use crate::checkpoint::{CheckpointId, TaskCheckpointState};
use parking_lot::RwLock;
use std::collections::HashMap;
use std::sync::Arc;

/// Abstract state backend trait.
pub trait StateBackend: Send + Sync {
    /// Get a state handle for a specific task.
    fn state_handle(&self, task_id: &str) -> Box<dyn StateHandle>;

    /// Backend name.
    fn name(&self) -> &str;
}

/// Handle for task-level state operations.
pub trait StateHandle: Send + Sync {
    /// Put a key-value pair.
    fn put(&self, key: &[u8], value: Vec<u8>);

    /// Get a value by key.
    fn get(&self, key: &[u8]) -> Option<Vec<u8>>;

    /// Delete a key.
    fn delete(&self, key: &[u8]);

    /// List all keys.
    fn keys(&self) -> Vec<Vec<u8>>;

    /// Take a snapshot for checkpointing.
    fn snapshot(&self, checkpoint_id: CheckpointId, timestamp: i64) -> TaskCheckpointState;

    /// Restore state from a snapshot.
    fn restore(&self, state: &TaskCheckpointState);
}

/// In-memory state backend (for testing and local mode).
pub struct MemoryStateBackend {
    states: Arc<RwLock<HashMap<String, HashMap<Vec<u8>, Vec<u8>>>>>,
}

impl MemoryStateBackend {
    pub fn new() -> Self {
        MemoryStateBackend {
            states: Arc::new(RwLock::new(HashMap::new())),
        }
    }
}

impl StateBackend for MemoryStateBackend {
    fn state_handle(&self, task_id: &str) -> Box<dyn StateHandle> {
        let mut states = self.states.write();
        states
            .entry(task_id.to_string())
            .or_insert_with(HashMap::new);
        Box::new(MemoryStateHandle {
            task_id: task_id.to_string(),
            states: self.states.clone(),
        })
    }

    fn name(&self) -> &str {
        "memory"
    }
}

/// In-memory state handle.
struct MemoryStateHandle {
    task_id: String,
    states: Arc<RwLock<HashMap<String, HashMap<Vec<u8>, Vec<u8>>>>>,
}

impl StateHandle for MemoryStateHandle {
    fn put(&self, key: &[u8], value: Vec<u8>) {
        let mut states = self.states.write();
        if let Some(task_state) = states.get_mut(&self.task_id) {
            task_state.insert(key.to_vec(), value);
        }
    }

    fn get(&self, key: &[u8]) -> Option<Vec<u8>> {
        let states = self.states.read();
        states.get(&self.task_id).and_then(|s| s.get(key).cloned())
    }

    fn delete(&self, key: &[u8]) {
        let mut states = self.states.write();
        if let Some(task_state) = states.get_mut(&self.task_id) {
            task_state.remove(key);
        }
    }

    fn keys(&self) -> Vec<Vec<u8>> {
        let states = self.states.read();
        states
            .get(&self.task_id)
            .map(|s| s.keys().cloned().collect())
            .unwrap_or_default()
    }

    fn snapshot(&self, checkpoint_id: CheckpointId, timestamp: i64) -> TaskCheckpointState {
        let states = self.states.read();
        let task_data: Vec<(Vec<u8>, Vec<u8>)> = states
            .get(&self.task_id)
            .map(|s| s.iter().map(|(k, v)| (k.clone(), v.clone())).collect())
            .unwrap_or_default();

        let json = serde_json::to_string(&task_data).unwrap_or_default();
        let mut state = TaskCheckpointState::new(self.task_id.clone(), checkpoint_id, timestamp);
        state.state_data = json.into_bytes();
        state.complete()
    }

    fn restore(&self, state: &TaskCheckpointState) {
        let pairs: Vec<(Vec<u8>, Vec<u8>)> =
            serde_json::from_slice(&state.state_data).unwrap_or_default();
        let task_data: HashMap<Vec<u8>, Vec<u8>> = pairs.into_iter().collect();
        let mut states = self.states.write();
        states.insert(self.task_id.clone(), task_data);
    }
}

/// Managed state backend with remote storage + local cache (stub).
pub struct ManagedStateBackend {
    memory: MemoryStateBackend,
    remote_base_path: String,
}

impl ManagedStateBackend {
    pub fn new(remote_base_path: &str) -> Self {
        ManagedStateBackend {
            memory: MemoryStateBackend::new(),
            remote_base_path: remote_base_path.to_string(),
        }
    }
}

impl StateBackend for ManagedStateBackend {
    fn state_handle(&self, task_id: &str) -> Box<dyn StateHandle> {
        self.memory.state_handle(task_id)
    }

    fn name(&self) -> &str {
        "managed"
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_memory_state_backend() {
        let backend = MemoryStateBackend::new();
        let handle = backend.state_handle("task-1");

        handle.put(b"key1", b"value1".to_vec());
        assert_eq!(handle.get(b"key1"), Some(b"value1".to_vec()));

        handle.delete(b"key1");
        assert_eq!(handle.get(b"key1"), None);
    }

    #[test]
    fn test_state_snapshot_restore() {
        let backend = MemoryStateBackend::new();
        let handle = backend.state_handle("task-1");

        handle.put(b"user_1", b"data_1".to_vec());
        handle.put(b"user_2", b"data_2".to_vec());

        let snapshot = handle.snapshot(1, 1000);
        assert!(snapshot.is_done);
        assert!(!snapshot.state_data.is_empty());

        // Restore into a new handle
        let backend2 = MemoryStateBackend::new();
        let handle2 = backend2.state_handle("task-1");
        handle2.restore(&snapshot);
        assert_eq!(handle2.get(b"user_1"), Some(b"data_1".to_vec()));
        assert_eq!(handle2.get(b"user_2"), Some(b"data_2".to_vec()));
    }

    #[test]
    fn test_managed_backend() {
        let backend = ManagedStateBackend::new("/remote/checkpoints");
        assert_eq!(backend.name(), "managed");

        let handle = backend.state_handle("t1");
        handle.put(b"k", b"v".to_vec());
        assert_eq!(handle.get(b"k"), Some(b"v".to_vec()));
    }

    #[test]
    fn test_keys() {
        let backend = MemoryStateBackend::new();
        let handle = backend.state_handle("t1");
        handle.put(b"a", b"1".to_vec());
        handle.put(b"b", b"2".to_vec());
        handle.put(b"c", b"3".to_vec());
        assert_eq!(handle.keys().len(), 3);
    }
}
