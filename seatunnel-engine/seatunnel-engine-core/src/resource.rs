/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.
 */

use std::collections::HashMap;

/// Resource description for a worker node.
#[derive(Debug, Clone)]
pub struct WorkerResource {
    /// Unique worker identifier.
    pub worker_id: String,
    /// Network address.
    pub address: String,
    /// Available CPU cores.
    pub cpu_cores: u32,
    /// Available memory in bytes.
    pub memory_bytes: u64,
    /// Additional resource metadata.
    pub labels: HashMap<String, String>,
}

impl WorkerResource {
    pub fn new(worker_id: String, address: String, cpu_cores: u32, memory_bytes: u64) -> Self {
        WorkerResource {
            worker_id,
            address,
            cpu_cores,
            memory_bytes,
            labels: HashMap::new(),
        }
    }

    /// Returns true if this worker has enough resources for a task.
    pub fn can_host_task(&self, required_cpu: u32, required_memory: u64) -> bool {
        self.cpu_cores >= required_cpu && self.memory_bytes >= required_memory
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_worker_resource() {
        let resource = WorkerResource::new(
            "w1".to_string(),
            "127.0.0.1:5001".to_string(),
            8,
            8 * 1024 * 1024 * 1024,
        );
        assert!(resource.can_host_task(2, 2 * 1024 * 1024 * 1024));
        assert!(!resource.can_host_task(16, 0));
    }
}
