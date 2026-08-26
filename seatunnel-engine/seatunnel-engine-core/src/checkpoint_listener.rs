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

//! Checkpoint notification hook fired by a TaskGroup whenever a checkpoint
//! completes locally.
//!
//! The engine server implements this trait to (a) persist the snapshot bytes
//! to the local state store and (b) report the checkpoint to the master via
//! gRPC. Keeping it as a trait lets tests inject in-memory collectors.

use std::future::Future;
use std::pin::Pin;

/// Receives completed task-level checkpoints.
pub trait CheckpointListener: Send + Sync {
    /// Called after a task flushed its sink and captured its source state.
    /// `state` contains the serialized reader state that a restart can
    /// restore from.
    fn on_checkpoint<'a>(
        &'a self,
        job_id: &'a str,
        task_id: &'a str,
        checkpoint_id: u64,
        timestamp: i64,
        state: Vec<u8>,
    ) -> Pin<Box<dyn Future<Output = ()> + Send + 'a>>;
}

/// No-op listener used when checkpointing notifications are not needed.
#[derive(Debug, Default, Clone, Copy)]
pub struct NopCheckpointListener;

impl CheckpointListener for NopCheckpointListener {
    fn on_checkpoint<'a>(
        &'a self,
        _job_id: &'a str,
        _task_id: &'a str,
        _checkpoint_id: u64,
        _timestamp: i64,
        _state: Vec<u8>,
    ) -> Pin<Box<dyn Future<Output = ()> + Send + 'a>> {
        Box::pin(async {})
    }
}
