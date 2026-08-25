/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.
 */

use std::fmt;

/// Execution mode for the engine.
#[derive(Debug, Clone, PartialEq)]
pub enum ExecutionMode {
    /// Single process: Master and Workers co-located in one JVM-equivalent.
    /// No network serialization overhead; tasks run in the same process.
    Local,

    /// Cluster mode: connect to external Master node(s).
    /// Tasks are scheduled across multiple worker nodes via gRPC.
    Cluster {
        /// Master node addresses (for high availability).
        addresses: Vec<String>,
        /// Client connection timeout in milliseconds.
        timeout_ms: u64,
    },
}

impl ExecutionMode {
    /// Returns true for Local execution.
    pub fn is_local(&self) -> bool {
        matches!(self, ExecutionMode::Local)
    }

    /// Returns true for Cluster execution.
    pub fn is_cluster(&self) -> bool {
        matches!(self, ExecutionMode::Cluster { .. })
    }

    /// Get the primary master address (for cluster mode).
    pub fn primary_address(&self) -> Option<String> {
        match self {
            ExecutionMode::Cluster { addresses, .. } => addresses.first().cloned(),
            ExecutionMode::Local => None,
        }
    }

    /// Get all master addresses (for cluster mode with HA).
    pub fn addresses(&self) -> Vec<String> {
        match self {
            ExecutionMode::Cluster { addresses, .. } => addresses.clone(),
            ExecutionMode::Local => vec!["127.0.0.1:5000".to_string()],
        }
    }
}

impl Default for ExecutionMode {
    fn default() -> Self {
        ExecutionMode::Local
    }
}

impl fmt::Display for ExecutionMode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ExecutionMode::Local => write!(f, "local"),
            ExecutionMode::Cluster { addresses, .. } => {
                write!(f, "cluster({})", addresses.join(","))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_local_mode() {
        let mode = ExecutionMode::Local;
        assert!(mode.is_local());
        assert!(!mode.is_cluster());
        assert_eq!(mode.primary_address(), None);
    }

    #[test]
    fn test_cluster_mode() {
        let mode = ExecutionMode::Cluster {
            addresses: vec!["127.0.0.1:5000".to_string(), "127.0.0.1:5001".to_string()],
            timeout_ms: 5000,
        };
        assert!(!mode.is_local());
        assert!(mode.is_cluster());
        assert_eq!(mode.primary_address(), Some("127.0.0.1:5000".to_string()));
        assert_eq!(mode.addresses().len(), 2);
    }

    #[test]
    fn test_display() {
        assert_eq!(ExecutionMode::Local.to_string(), "local");
    }
}
