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

//! Task execution group: Source → Transform → Sink chained pipeline.
//!
//! Mirrors Java's SeaTunnelTask + FlowLifeCycle chain: one TaskGroup owns a
//! source reader, an optional transform chain and a sink writer, and pumps
//! records between them. This is the unit of execution the cluster worker and
//! the CLI local runner both drive.
//!
//! Checkpoint protocol (per checkpoint interval):
//!   1. `sink.prepare_commit()` — downstream data is flushed **first**
//!   2. `reader.snapshot_state()` — source offset captured after the flush
//!   3. listener notified with the serialized state (persist + report)
//! Because the sink is flushed before the offset is recorded, a restart from
//! any completed checkpoint replays at least once without losing records.

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use seatunnel_api::row::Row;
use seatunnel_api::schema::TableSchema;
use seatunnel_api::source::source_reader::{PollResult, SourceReader};
use seatunnel_api::transform::Transform;
use seatunnel_api::sink::SinkWriter;

use crate::barrier::{BarrierTracker, CheckpointBarrier, StreamElement};
use crate::checkpoint::CheckpointConfig;
use crate::checkpoint_listener::CheckpointListener;
use crate::connector_factory::{AnySplit, BoxedSinkWriter, BoxedSourceReader, BoxedTransform};
use crate::task::{TaskId, TaskStatus};
use crate::state::TaskState;

/// Context passed to each task during execution.
#[derive(Clone)]
pub struct TaskContext {
    pub task_id: TaskId,
    pub job_id: String,
    pub stage_id: String,
    pub subtask_index: usize,
    pub parallelism: usize,
    pub checkpoint_config: CheckpointConfig,
    /// Cooperative cancellation handle; polled every loop iteration.
    pub cancel_token: Option<Arc<tokio_util::sync::CancellationToken>>,
    /// Receives completed checkpoints (persist + master report).
    pub checkpoint_listener: Option<Arc<dyn CheckpointListener>>,
}

impl TaskContext {
    pub fn new(
        task_id: impl Into<String>,
        job_id: impl Into<String>,
        stage_id: impl Into<String>,
        subtask_index: usize,
        parallelism: usize,
    ) -> Self {
        TaskContext {
            task_id: task_id.into(),
            job_id: job_id.into(),
            stage_id: stage_id.into(),
            subtask_index,
            parallelism,
            checkpoint_config: CheckpointConfig::default(),
            cancel_token: None,
            checkpoint_listener: None,
        }
    }

    pub fn with_cancel_token(mut self, token: Arc<tokio_util::sync::CancellationToken>) -> Self {
        self.cancel_token = Some(token);
        self
    }

    pub fn with_checkpoint_interval(mut self, interval_ms: u64) -> Self {
        self.checkpoint_config.interval_ms = interval_ms.max(1);
        self
    }

    pub fn with_checkpoint_listener(
        mut self,
        listener: Arc<dyn CheckpointListener>,
    ) -> Self {
        self.checkpoint_listener = Some(listener);
        self
    }
}

/// The main task execution group over type-erased connectors.
pub struct TaskGroup {
    context: TaskContext,
    reader: BoxedSourceReader,
    transforms: Vec<BoxedTransform>,
    output_schema: Option<TableSchema>,
    sink: BoxedSinkWriter,
    status: Arc<tokio::sync::Mutex<TaskStatus>>,
    records_processed: u64,
    checkpoints_completed: u64,
    last_checkpoint_at: Option<i64>,
}

impl TaskGroup {
    pub fn new(context: TaskContext, reader: BoxedSourceReader, sink: BoxedSinkWriter) -> Self {
        let task_id = context.task_id.clone();
        TaskGroup {
            context,
            reader,
            transforms: Vec::new(),
            output_schema: None,
            sink,
            status: Arc::new(tokio::sync::Mutex::new(TaskStatus::new(task_id))),
            records_processed: 0,
            checkpoints_completed: 0,
            last_checkpoint_at: None,
        }
    }

    pub fn with_transforms(mut self, transforms: Vec<BoxedTransform>) -> Self {
        self.transforms = transforms;
        self
    }

    pub fn with_output_schema(mut self, schema: TableSchema) -> Self {
        self.output_schema = Some(schema);
        self
    }

    pub fn status(&self) -> Arc<tokio::sync::Mutex<TaskStatus>> {
        self.status.clone()
    }

    /// Number of checkpoints this group completed successfully so far.
    pub fn checkpoints_completed(&self) -> u64 {
        self.checkpoints_completed
    }

    /// Run the task execution loop until EOF, cancellation or failure.
    pub async fn run(&mut self) -> anyhow::Result<TaskStatus> {
        {
            let mut status = self.status.lock().await;
            status.state = TaskState::Running;
            status.start_time = crate::now_millis();
        }

        // Open the source reader, then the sink writer.
        self.reader.open().await?;
        self.sink.open().await?;

        let mut barrier_tracker = BarrierTracker::new(
            self.context.task_id.clone(),
            self.context.parallelism,
        );

        let mut terminal_state = TaskState::Completed;

        loop {
            // Cooperative cancellation.
            if let Some(token) = &self.context.cancel_token {
                if token.is_cancelled() {
                    tracing::info!("Task {} cancelled by coordinator", self.context.task_id);
                    terminal_state = TaskState::Cancelled;
                    break;
                }
            }

            // Periodic checkpoint: flush sink BEFORE capturing source state.
            if let Some(cp_id) = self.maybe_trigger_checkpoint().await? {
                let now = crate::now_millis();
                let barrier = CheckpointBarrier::new(cp_id, now);
                barrier_tracker.receive(StreamElement::CheckpointBarrier(barrier));
                self.checkpoints_completed += 1;
                self.last_checkpoint_at = Some(now);
            }

            match self.reader.poll_next().await {
                Ok(PollResult::Record(output)) => {
                    let rows = self.apply_transforms(output)?;
                    self.records_processed += rows.len() as u64;
                    {
                        let mut status = self.status.lock().await;
                        status.processed_records = self.records_processed;
                    }
                    for row in rows {
                        self.sink.write(row).await?;
                    }
                }
                Ok(PollResult::Empty) => {
                    tokio::time::sleep(Duration::from_millis(20)).await;
                }
                Ok(PollResult::EOF) => break,
                Err(e) => {
                    terminal_state = TaskState::Failed { error: e.to_string() };
                    tracing::error!("Task {} failed in poll loop: {}", self.context.task_id, e);
                    break;
                }
            }
        }

        if terminal_state == TaskState::Completed || terminal_state == TaskState::Cancelled {
            // Final flush of whatever the sink still buffers.
            if let Err(e) = self.sink.prepare_commit().await {
                tracing::error!("Task {} final prepare_commit failed: {}", self.context.task_id, e);
                if terminal_state == TaskState::Completed {
                    terminal_state = TaskState::Failed { error: format!("final sink flush failed: {}", e) };
                }
            }
        }

        // Close resources; never mask the terminal state on close errors.
        if let Err(e) = self.reader.close().await {
            tracing::warn!("Task {} reader close error: {}", self.context.task_id, e);
        }
        if let Err(e) = self.sink.close().await {
            tracing::warn!("Task {} sink close error: {}", self.context.task_id, e);
        }

        {
            let mut status = self.status.lock().await;
            if let TaskState::Failed { ref error } = terminal_state {
                status.error = Some(error.clone());
            }
            status.state = terminal_state;
            status.end_time = crate::now_millis();
            status.processed_records = self.records_processed;
        }

        tracing::info!(
            "Task {} finished: state={} records={} checkpoints={}",
            self.context.task_id,
            self.status.lock().await.state,
            self.records_processed,
            self.checkpoints_completed
        );

        Ok(self.status.lock().await.clone())
    }

    /// Trigger a checkpoint when the configured interval has elapsed.
    /// Returns the checkpoint id on success.
    async fn maybe_trigger_checkpoint(&mut self) -> anyhow::Result<Option<u64>> {
        if self.context.checkpoint_listener.is_none() {
            return Ok(None);
        }
        let now = crate::now_millis();
        let due = match self.last_checkpoint_at {
            Some(last) => now - last >= self.context.checkpoint_config.interval_ms as i64,
            None => true,
        };
        if !due {
            return Ok(None);
        }
        let cp_id = self.checkpoints_completed + 1;

        // 1. Flush downstream first — everything emitted before this point
        //    must be visible before we record where the source stands.
        self.sink.prepare_commit().await?;

        // 2. Capture the source state after the flush.
        let state = self.reader.snapshot_state().await.map_err(|e| {
            anyhow::anyhow!("reader snapshot_state failed: {}", e)
        })?;

        // 3. Persist + report via the listener.
        if let Some(listener) = &self.context.checkpoint_listener {
            listener
                .on_checkpoint(&self.context.job_id, &self.context.task_id, cp_id, now, state)
                .await;
        }

        tracing::debug!(
            "Task {} checkpoint {} completed (records={})",
            self.context.task_id,
            cp_id,
            self.records_processed
        );
        Ok(Some(cp_id))
    }

    /// Apply transform chain to a single row.
    fn apply_transforms(&mut self, row: Row) -> anyhow::Result<Vec<Row>> {
        let mut current = vec![row];
        for transform in &mut self.transforms {
            let mut next = Vec::new();
            for r in current {
                next.extend(transform.process(r)?);
            }
            current = next;
        }
        Ok(current)
    }
}

/// Run a local pipeline: Source → Transforms → Sink (used by tests).
pub async fn run_local_pipeline(
    context: TaskContext,
    source: BoxedSourceReader,
    transforms: Vec<BoxedTransform>,
    sink: BoxedSinkWriter,
) -> anyhow::Result<TaskStatus> {
    let mut group = TaskGroup::new(context, source, sink).with_transforms(transforms);
    group.run().await
}
