/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/// Execution mode for a SeaTunnel job.
///
/// Mirrors Java's `ExecutionMode` and `MasterType`.
/// - `Local`: Single process, embedded Master + Worker (对应 `-m local`)
/// - `Cluster`: Connect to an external cluster (对应 `-m cluster`)
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub enum ExecutionMode {
    /// Local mode: single process acting as both master and worker.
    /// No network communication needed. Fast startup, ideal for development
    /// and small batch jobs.
    #[default]
    Local,

    /// Cluster mode: connect to an external distributed cluster.
    /// Requires at least one cluster node to be running.
    Cluster {
        /// List of cluster node addresses (host:port).
        addresses: Vec<String>,
    },
}

impl std::fmt::Display for ExecutionMode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ExecutionMode::Local => write!(f, "local"),
            ExecutionMode::Cluster { addresses } => {
                write!(f, "cluster({})", addresses.join(","))
            }
        }
    }
}
