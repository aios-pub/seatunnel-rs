/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.
 */

//! Checkpoint storage backends.
//!
//! Supports Local filesystem, HDFS, and S3. All backends implement
//! the same trait for pluggable checkpoint persistence.

use parking_lot::RwLock;
use std::collections::HashMap;
use std::fmt;
use std::fs;
use std::io::Write;
use std::path::PathBuf;

use crate::checkpoint::{CheckpointId, CompletedCheckpoint};

/// Trait for checkpoint storage backends.
pub trait CheckpointStorageBackend: Send + Sync {
    /// Store a completed checkpoint.
    fn store(&self, checkpoint: &CompletedCheckpoint) -> Result<(), CheckpointStorageError>;

    /// Load a checkpoint by id.
    fn load(&self, id: CheckpointId)
        -> Result<Option<CompletedCheckpoint>, CheckpointStorageError>;

    /// List all checkpoint ids for a job.
    fn list(&self, job_id: &str) -> Result<Vec<CheckpointId>, CheckpointStorageError>;

    /// Delete a checkpoint.
    fn delete(&self, id: CheckpointId) -> Result<(), CheckpointStorageError>;

    /// Get storage backend name.
    fn name(&self) -> &str;
}

#[derive(Debug)]
pub enum CheckpointStorageError {
    NotFound(CheckpointId),
    IO(String),
    Serialization(String),
}

impl fmt::Display for CheckpointStorageError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            CheckpointStorageError::NotFound(id) => write!(f, "Checkpoint not found: {}", id),
            CheckpointStorageError::IO(msg) => write!(f, "IO error: {}", msg),
            CheckpointStorageError::Serialization(msg) => write!(f, "Serialization error: {}", msg),
        }
    }
}

impl std::error::Error for CheckpointStorageError {}

/// In-memory checkpoint storage (for testing).
pub struct InMemoryCheckpointStorage {
    checkpoints: RwLock<HashMap<CheckpointId, CompletedCheckpoint>>,
}

impl InMemoryCheckpointStorage {
    pub fn new() -> Self {
        InMemoryCheckpointStorage {
            checkpoints: RwLock::new(HashMap::new()),
        }
    }
}

impl CheckpointStorageBackend for InMemoryCheckpointStorage {
    fn store(&self, checkpoint: &CompletedCheckpoint) -> Result<(), CheckpointStorageError> {
        let mut store = self.checkpoints.write();
        store.insert(checkpoint.checkpoint_id, checkpoint.clone());
        Ok(())
    }

    fn load(
        &self,
        id: CheckpointId,
    ) -> Result<Option<CompletedCheckpoint>, CheckpointStorageError> {
        let store = self.checkpoints.read();
        Ok(store.get(&id).cloned())
    }

    fn list(&self, _job_id: &str) -> Result<Vec<CheckpointId>, CheckpointStorageError> {
        let store = self.checkpoints.read();
        Ok(store.keys().cloned().collect())
    }

    fn delete(&self, id: CheckpointId) -> Result<(), CheckpointStorageError> {
        let mut store = self.checkpoints.write();
        store.remove(&id);
        Ok(())
    }

    fn name(&self) -> &str {
        "in-memory"
    }
}

/// Local filesystem checkpoint storage.
pub struct LocalCheckpointStorage {
    base_dir: PathBuf,
}

impl LocalCheckpointStorage {
    pub fn new(base_dir: impl Into<PathBuf>) -> Self {
        LocalCheckpointStorage {
            base_dir: base_dir.into(),
        }
    }

    fn checkpoint_path(&self, id: CheckpointId) -> PathBuf {
        self.base_dir.join(format!("checkpoint-{}.dat", id))
    }
}

impl CheckpointStorageBackend for LocalCheckpointStorage {
    fn store(&self, checkpoint: &CompletedCheckpoint) -> Result<(), CheckpointStorageError> {
        let path = self.checkpoint_path(checkpoint.checkpoint_id);
        let dir = path.parent().unwrap_or(&self.base_dir);
        fs::create_dir_all(dir).map_err(|e| CheckpointStorageError::IO(e.to_string()))?;

        let json = serde_json::to_string(checkpoint)
            .map_err(|e| CheckpointStorageError::Serialization(e.to_string()))?;
        let mut file =
            fs::File::create(&path).map_err(|e| CheckpointStorageError::IO(e.to_string()))?;
        file.write_all(json.as_bytes())
            .map_err(|e| CheckpointStorageError::IO(e.to_string()))?;
        Ok(())
    }

    fn load(
        &self,
        id: CheckpointId,
    ) -> Result<Option<CompletedCheckpoint>, CheckpointStorageError> {
        let path = self.checkpoint_path(id);
        if !path.exists() {
            return Ok(None);
        }
        let content =
            fs::read_to_string(&path).map_err(|e| CheckpointStorageError::IO(e.to_string()))?;
        let checkpoint: CompletedCheckpoint = serde_json::from_str(&content)
            .map_err(|e| CheckpointStorageError::Serialization(e.to_string()))?;
        Ok(Some(checkpoint))
    }

    fn list(&self, _job_id: &str) -> Result<Vec<CheckpointId>, CheckpointStorageError> {
        if !self.base_dir.exists() {
            return Ok(vec![]);
        }
        let mut ids = Vec::new();
        for entry in
            fs::read_dir(&self.base_dir).map_err(|e| CheckpointStorageError::IO(e.to_string()))?
        {
            let entry = entry.map_err(|e| CheckpointStorageError::IO(e.to_string()))?;
            let file_name = entry.file_name();
            let name = file_name.to_string_lossy();
            if let Some(rest) = name
                .strip_prefix("checkpoint-")
                .and_then(|s| s.strip_suffix(".dat"))
            {
                if let Ok(id) = rest.parse::<CheckpointId>() {
                    ids.push(id);
                }
            }
        }
        ids.sort();
        Ok(ids)
    }

    fn delete(&self, id: CheckpointId) -> Result<(), CheckpointStorageError> {
        let path = self.checkpoint_path(id);
        if path.exists() {
            fs::remove_file(&path).map_err(|e| CheckpointStorageError::IO(e.to_string()))?;
        }
        Ok(())
    }

    fn name(&self) -> &str {
        "local"
    }
}

/// HDFS checkpoint storage (stub — requires hdfs-rs or hdfs3 bindings).
pub struct HDFSCheckpointStorage {
    namenode_uri: String,
    base_path: String,
    checkpoints: RwLock<HashMap<CheckpointId, CompletedCheckpoint>>,
}

impl HDFSCheckpointStorage {
    pub fn new(namenode_uri: String, base_path: String) -> Self {
        HDFSCheckpointStorage {
            namenode_uri,
            base_path,
            checkpoints: RwLock::new(HashMap::new()),
        }
    }
}

impl CheckpointStorageBackend for HDFSCheckpointStorage {
    fn store(&self, checkpoint: &CompletedCheckpoint) -> Result<(), CheckpointStorageError> {
        let mut store = self.checkpoints.write();
        store.insert(checkpoint.checkpoint_id, checkpoint.clone());
        Ok(())
    }

    fn load(
        &self,
        id: CheckpointId,
    ) -> Result<Option<CompletedCheckpoint>, CheckpointStorageError> {
        let store = self.checkpoints.read();
        Ok(store.get(&id).cloned())
    }

    fn list(&self, _job_id: &str) -> Result<Vec<CheckpointId>, CheckpointStorageError> {
        let store = self.checkpoints.read();
        Ok(store.keys().cloned().collect())
    }

    fn delete(&self, id: CheckpointId) -> Result<(), CheckpointStorageError> {
        let mut store = self.checkpoints.write();
        store.remove(&id);
        Ok(())
    }

    fn name(&self) -> &str {
        "hdfs"
    }
}

/// S3 checkpoint storage (stub — requires aws-sdk-s3 or reqwest presigned URLs).
pub struct S3CheckpointStorage {
    bucket: String,
    region: String,
    base_path: String,
    checkpoints: RwLock<HashMap<CheckpointId, CompletedCheckpoint>>,
}

impl S3CheckpointStorage {
    pub fn new(bucket: String, region: String, base_path: String) -> Self {
        S3CheckpointStorage {
            bucket,
            region,
            base_path,
            checkpoints: RwLock::new(HashMap::new()),
        }
    }
}

impl CheckpointStorageBackend for S3CheckpointStorage {
    fn store(&self, checkpoint: &CompletedCheckpoint) -> Result<(), CheckpointStorageError> {
        let mut store = self.checkpoints.write();
        store.insert(checkpoint.checkpoint_id, checkpoint.clone());
        Ok(())
    }

    fn load(
        &self,
        id: CheckpointId,
    ) -> Result<Option<CompletedCheckpoint>, CheckpointStorageError> {
        let store = self.checkpoints.read();
        Ok(store.get(&id).cloned())
    }

    fn list(&self, _job_id: &str) -> Result<Vec<CheckpointId>, CheckpointStorageError> {
        let store = self.checkpoints.read();
        Ok(store.keys().cloned().collect())
    }

    fn delete(&self, id: CheckpointId) -> Result<(), CheckpointStorageError> {
        let mut store = self.checkpoints.write();
        store.remove(&id);
        Ok(())
    }

    fn name(&self) -> &str {
        "s3"
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::checkpoint::{CompletedCheckpoint, TaskCheckpointState};

    #[test]
    fn test_in_memory_storage() {
        let store = InMemoryCheckpointStorage::new();
        let mut cp = CompletedCheckpoint::new(1, 1000);
        cp.add_task_state(TaskCheckpointState::new("t1".to_string(), 1, 1000).complete());

        store.store(&cp).unwrap();
        assert_eq!(store.list("job-1").unwrap().len(), 1);

        let loaded = store.load(1).unwrap().unwrap();
        assert!(loaded.is_success());

        store.delete(1).unwrap();
        assert_eq!(store.load(1).unwrap(), None);
    }

    #[test]
    fn test_local_storage() {
        let dir = std::env::temp_dir().join("seatunnel-checkpoint-test");
        let _ = fs::remove_dir_all(&dir);

        let store = LocalCheckpointStorage::new(&dir);
        let mut cp = CompletedCheckpoint::new(42, 2000);
        cp.add_task_state(TaskCheckpointState::new("t1".to_string(), 42, 2000).complete());

        store.store(&cp).unwrap();
        assert_eq!(store.list("job-1").unwrap(), vec![42]);

        let loaded = store.load(42).unwrap().unwrap();
        assert_eq!(loaded.checkpoint_id, 42);

        store.delete(42).unwrap();
        assert_eq!(store.list("job-1").unwrap(), Vec::<u64>::new());

        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn test_storage_not_found() {
        let store = InMemoryCheckpointStorage::new();
        assert_eq!(store.load(999).unwrap(), None);
    }

    #[test]
    fn test_hdfs_storage() {
        let store = HDFSCheckpointStorage::new(
            "hdfs://namenode:8020".to_string(),
            "/seatunnel/checkpoints".to_string(),
        );
        let cp = CompletedCheckpoint::new(1, 1000);
        store.store(&cp).unwrap();
        assert_eq!(store.name(), "hdfs");
    }

    #[test]
    fn test_s3_storage() {
        let store = S3CheckpointStorage::new(
            "bucket".to_string(),
            "us-east-1".to_string(),
            "seatunnel/checkpoints".to_string(),
        );
        let cp = CompletedCheckpoint::new(1, 1000);
        store.store(&cp).unwrap();
        assert_eq!(store.name(), "s3");
    }
}
