/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.
 */

//! Engine Core: DAG model, ExecutionMode, ResourceManager, Checkpoint types.
//!
//! This is the foundational data model for the distributed engine.

use chrono::Utc;
use uuid::Uuid;

pub mod barrier;
pub mod checkpoint;
pub mod checkpoint_listener;
#[cfg(feature = "connectors")]
pub mod connector_factory;
pub mod dag;
pub mod execution;
pub mod fanout;
pub mod local_checkpoint;
pub mod recovery;
pub mod resource;
pub mod state;
pub mod task;
pub mod task_group;
pub mod task_log;

// Re-export key types
pub use barrier::{BarrierTracker, CheckpointBarrier, StreamElement};
pub use checkpoint::{
    CheckpointConfig, CheckpointId, CheckpointState, CheckpointStorage, CompletedCheckpoint,
    TaskCheckpointState,
};
pub use checkpoint_listener::{CheckpointListener, NopCheckpointListener};
#[cfg(feature = "connectors")]
pub use connector_factory::{
    AnySplit, BoxedSinkWriter, BoxedSourceReader, BoxedTransform, ConsoleSinkWriter, FakeSeqSource,
    create_sink, create_source, create_transforms,
};
pub use dag::{Pipeline, Stage, StageType};
pub use execution::ExecutionMode;
pub use recovery::{FailureEvent, RecoveryAction, RecoveryManager, RecoveryPlan, RecoveryState};
pub use resource::WorkerResource;
pub use state::{JobState, TaskState};
pub use task::{TaskId, TaskKind, TaskStatus};

/// Unique job identifier.
pub type JobId = String;

/// Unique stage identifier within a job.
pub type StageId = String;

/// Generate a unique job id.
pub fn generate_job_id() -> JobId {
    Uuid::new_v4().to_string()
}

/// Generate a unique stage id.
pub fn generate_stage_id() -> StageId {
    format!("stage-{}", Uuid::new_v4().simple())
}

/// Current timestamp in milliseconds.
pub fn now_millis() -> i64 {
    Utc::now().timestamp_millis()
}
