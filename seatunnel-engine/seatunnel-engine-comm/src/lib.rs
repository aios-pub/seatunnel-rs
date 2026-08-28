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

//! Engine Communication: gRPC service layer for Master/Worker/Client communication.
//!
//! Generated from `src/proto/master.proto` via tonic-build.

pub mod generated {
    tonic::include_proto!("seatunnel.engine.v1");
}

pub use generated::client_service_client::ClientServiceClient;
pub use generated::client_service_server::ClientService;
pub use generated::master_service_client::MasterServiceClient;
pub use generated::master_service_server::MasterService;
pub use generated::raft_service_client::RaftServiceClient;
pub use generated::raft_service_server::{RaftService, RaftServiceServer};
pub use generated::{
    CancelJobRequest, CheckpointEntry, CheckpointPhase, CheckpointReport, CheckpointResolution,
    CheckpointTrigger, ClusterInfo, Empty, FetchCheckpointRequest, FetchCheckpointResponse,
    HeartbeatRequest, HeartbeatResponse, JobCheckpointHistory, JobList, JobLogs, JobStatus,
    JobStatusRequest, JobSummary, SubmitJobRequest,
    SubmitJobResponse, TaskCheckpointHistory, TaskDescriptor, TaskHeartbeat, TaskLogs,
    TaskStatusInfo, TaskStatusReport, UnregisterWorkerRequest, WorkerInfo, WorkerRegistration,
    WorkerRegistrationResponse,
};

use generated::{JobState as ProtoJobState, TaskState as ProtoTaskState};

use seatunnel_engine_core::state::{JobState as CoreJobState, TaskState as CoreTaskState};

/// Converts a core TaskState to the protobuf TaskState.
pub fn task_state_to_proto(state: &CoreTaskState) -> i32 {
    match state {
        CoreTaskState::Created => ProtoTaskState::TaskCreated as i32,
        CoreTaskState::Running => ProtoTaskState::TaskRunning as i32,
        CoreTaskState::Completed => ProtoTaskState::TaskCompleted as i32,
        CoreTaskState::Failed { .. } => ProtoTaskState::TaskFailed as i32,
        CoreTaskState::Cancelled => ProtoTaskState::TaskCancelled as i32,
    }
}

/// Converts a protobuf TaskState to a core TaskState.
pub fn task_state_from_proto(proto: i32) -> CoreTaskState {
    match proto {
        x if x == ProtoTaskState::TaskRunning as i32 => CoreTaskState::Running,
        x if x == ProtoTaskState::TaskCompleted as i32 => CoreTaskState::Completed,
        x if x == ProtoTaskState::TaskFailed as i32 => CoreTaskState::Failed {
            error: "unknown".to_string(),
        },
        x if x == ProtoTaskState::TaskCancelled as i32 => CoreTaskState::Cancelled,
        _ => CoreTaskState::Created,
    }
}

/// Converts a core JobState to the protobuf JobState.
pub fn job_state_to_proto(state: &CoreJobState) -> i32 {
    match state {
        CoreJobState::Created => ProtoJobState::JobCreated as i32,
        CoreJobState::Scheduled => ProtoJobState::JobScheduled as i32,
        CoreJobState::Running => ProtoJobState::JobRunning as i32,
        CoreJobState::Completed => ProtoJobState::JobCompleted as i32,
        CoreJobState::Failed { .. } => ProtoJobState::JobFailed as i32,
        CoreJobState::Cancelled => ProtoJobState::JobCancelled as i32,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_task_state_conversion() {
        assert_eq!(
            task_state_to_proto(&CoreTaskState::Running),
            ProtoTaskState::TaskRunning as i32
        );
        assert_eq!(
            task_state_from_proto(ProtoTaskState::TaskRunning as i32),
            CoreTaskState::Running
        );
    }

    #[test]
    fn test_job_state_conversion() {
        assert_eq!(
            job_state_to_proto(&CoreJobState::Running),
            ProtoJobState::JobRunning as i32
        );
    }
}
