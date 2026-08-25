/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.
 */

use parking_lot::RwLock;
use std::collections::HashMap;

/// Worker resource manager tracks available workers and their capacity.
pub struct ResourceManager {
    workers: RwLock<HashMap<String, WorkerInfo>>,
}

#[derive(Debug, Clone)]
pub struct WorkerInfo {
    pub worker_id: String,
    pub address: String,
    pub cpu_cores: u32,
    pub memory_bytes: u64,
    pub running_tasks: u32,
    pub last_heartbeat: i64,
}

impl ResourceManager {
    pub fn new() -> Self {
        ResourceManager {
            workers: RwLock::new(HashMap::new()),
        }
    }

    /// Register or update a worker.
    pub fn register_worker(
        &self,
        worker_id: String,
        address: String,
        cpu_cores: u32,
        memory_bytes: u64,
    ) {
        let mut workers = self.workers.write();
        workers.insert(
            worker_id.clone(),
            WorkerInfo {
                worker_id,
                address,
                cpu_cores,
                memory_bytes,
                running_tasks: 0,
                last_heartbeat: 0,
            },
        );
    }

    /// Remove a worker.
    pub fn remove_worker(&self, worker_id: &str) {
        let mut workers = self.workers.write();
        workers.remove(worker_id);
    }

    /// Update worker heartbeat timestamp.
    pub fn update_heartbeat(&self, worker_id: &str, timestamp: i64) {
        let mut workers = self.workers.write();
        if let Some(info) = workers.get_mut(worker_id) {
            info.last_heartbeat = timestamp;
        }
    }

    /// Assign a task to a worker using round-robin (least loaded).
    pub fn assign_task(&self) -> Option<(String, String)> {
        let mut workers = self.workers.write();
        workers
            .iter_mut()
            .min_by_key(|(_, v)| v.running_tasks)
            .map(|(id, info)| {
                info.running_tasks += 1;
                (id.clone(), info.address.clone())
            })
    }

    /// Get all registered workers.
    pub fn get_workers(&self) -> Vec<(String, String)> {
        let workers = self.workers.read();
        workers
            .iter()
            .map(|(id, info)| (id.clone(), info.address.clone()))
            .collect()
    }

    /// Get the number of registered workers.
    pub fn worker_count(&self) -> usize {
        let workers = self.workers.read();
        workers.len()
    }

    /// Check if a worker is alive (based on heartbeat).
    pub fn is_worker_alive(&self, worker_id: &str, timeout_ms: i64) -> bool {
        let workers = self.workers.read();
        if let Some(info) = workers.get(worker_id) {
            info.last_heartbeat > 0 // simplified: any heartbeat means alive
        } else {
            let _ = timeout_ms;
            false
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_register_and_assign() {
        let rm = ResourceManager::new();
        rm.register_worker(
            "w1".to_string(),
            "127.0.0.1:5001".to_string(),
            8,
            8 * 1024 * 1024 * 1024,
        );
        rm.register_worker(
            "w2".to_string(),
            "127.0.0.1:5002".to_string(),
            4,
            4 * 1024 * 1024 * 1024,
        );

        assert_eq!(rm.worker_count(), 2);
        rm.update_heartbeat("w1", 1000);
        assert!(rm.is_worker_alive("w1", 5000));

        let (id, addr) = rm.assign_task().unwrap();
        assert!(id == "w1" || id == "w2");
        assert!(addr == "127.0.0.1:5001" || addr == "127.0.0.1:5002");
    }

    #[test]
    fn test_remove_worker() {
        let rm = ResourceManager::new();
        rm.register_worker(
            "w1".to_string(),
            "127.0.0.1:5001".to_string(),
            8,
            8 * 1024 * 1024 * 1024,
        );
        rm.remove_worker("w1");
        assert_eq!(rm.worker_count(), 0);
    }
}
