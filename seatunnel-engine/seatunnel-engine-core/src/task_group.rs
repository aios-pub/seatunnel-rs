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
//! Checkpoint protocol — coordinator-driven (local mode, preferred):
//!   1. the checkpoint driver triggers a global checkpoint id
//!   2. this task cuts a barrier between polls: `sink.prepare_commit(id)`
//!      → `sink.snapshot_state()` → `reader.snapshot_state()`
//!   3. the driver persists every task's payloads as one envelope and
//!      broadcasts completion; the task then runs 2PC phase 2
//!      (`SinkCommitter::commit`) and `reader.notify_checkpoint_complete`
//!
//! Legacy interval protocol (cluster worker): per task, on its own clock,
//!   1. `sink.prepare_commit()` — downstream data is flushed **first**
//!   2. `reader.snapshot_state()` — source offset captured after the flush
//!   3. listener notified with the serialized state (persist + report)
//!      Because the sink is flushed before the offset is recorded, a restart
//!      from any completed checkpoint replays at least once without losing
//!      records.

use std::sync::Arc;
use std::time::{Duration, Instant};

use seatunnel_api::row::Row;
use seatunnel_api::schema::TableSchema;
use seatunnel_api::source::source_reader::PollResult;

use crate::barrier::{BarrierTracker, CheckpointBarrier, StreamElement};
use crate::checkpoint::CheckpointConfig;
use crate::checkpoint_listener::CheckpointListener;
use crate::connector_factory::{BoxedSinkCommitter, BoxedSinkWriter, BoxedSourceReader, BoxedTransform};
use crate::local_checkpoint::{CheckpointEvent, TaskToDriver};
use crate::state::TaskState;
use crate::task::{TaskId, TaskStatus};

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
    /// Coordinator-driven checkpointing (local mode): receives barrier
    /// triggers and completion events from the checkpoint driver.
    pub checkpoint_handle: Option<crate::local_checkpoint::CheckpointHandle>,
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
            checkpoint_handle: None,
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

    pub fn with_checkpoint_listener(mut self, listener: Arc<dyn CheckpointListener>) -> Self {
        self.checkpoint_listener = Some(listener);
        self
    }

    pub fn with_checkpoint_handle(
        mut self,
        handle: crate::local_checkpoint::CheckpointHandle,
    ) -> Self {
        self.checkpoint_handle = Some(handle);
        self
    }
}

/// Live status publish interval for the shared `TaskStatus`. Locking the
/// status mutex on every record dominated the hot loop; observers only need
/// a near-real-time view (the exact final count is published on exit).
const STATUS_PUBLISH_INTERVAL: Duration = Duration::from_millis(200);

/// Idle backoff schedule after `PollResult::Empty`. The first miss yields
/// immediately (data often arrives within the same scheduler tick), then
/// sleeps escalate to a small cap so idle CPU stays bounded without adding
/// fixed latency when traffic resumes.
const IDLE_BACKOFF_MS: [u64; 5] = [1, 2, 5, 10, 20];

/// Checkpoint id used for the final checkpoint at task exit (Java's
/// `Barrier#PREPARE_CLOSE_BARRIER_ID` semantics: a last durable snapshot
/// so a restart resumes where the job stopped instead of replaying).
pub const FINAL_CHECKPOINT_ID: u64 = u64::MAX - 1;

/// The main task execution group over type-erased connectors.
pub struct TaskGroup {
    context: TaskContext,
    reader: BoxedSourceReader,
    transforms: Vec<BoxedTransform>,
    output_schema: Option<TableSchema>,
    sink: BoxedSinkWriter,
    /// Optional 2PC committer (phase 2), driven on checkpoint completion.
    committer: Option<BoxedSinkCommitter>,
    /// Commit infos returned by the last barrier, input for phase 2.
    last_commit_infos: Vec<Vec<u8>>,
    status: Arc<tokio::sync::Mutex<TaskStatus>>,
    records_processed: u64,
    checkpoints_completed: u64,
    last_checkpoint_at: Option<i64>,
    last_status_publish: Option<Instant>,
    empty_streak: u32,
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
            committer: None,
            last_commit_infos: Vec::new(),
            status: Arc::new(tokio::sync::Mutex::new(TaskStatus::new(task_id))),
            records_processed: 0,
            checkpoints_completed: 0,
            last_checkpoint_at: None,
            last_status_publish: None,
            empty_streak: 0,
        }
    }

    pub fn with_transforms(mut self, transforms: Vec<BoxedTransform>) -> Self {
        self.transforms = transforms;
        self
    }

    pub fn with_committer(mut self, committer: Option<BoxedSinkCommitter>) -> Self {
        self.committer = committer;
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

        let mut barrier_tracker =
            BarrierTracker::new(self.context.task_id.clone(), self.context.parallelism);

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

            // Coordinator-driven checkpoint barriers (local checkpointing):
            // cut between polls so the sink commit and the source offset
            // refer to the same record prefix.
            if let Some(handle) = self.context.checkpoint_handle.clone() {
                let mut event_error = None;
                while let Some(cp_id) = handle.take_trigger() {
                    if let Err(e) = self.execute_barrier(cp_id).await {
                        handle.report(TaskToDriver::CheckpointFailed {
                            task_id: self.context.task_id.clone(),
                            checkpoint_id: cp_id,
                            error: e.to_string(),
                        });
                    }
                }
                while let Some(event) = handle.poll_event() {
                    if let Err(e) = self.handle_checkpoint_event(event).await {
                        event_error = Some(e.to_string());
                        break;
                    }
                }
                if let Some(error) = event_error {
                    terminal_state = TaskState::Failed { error };
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
                    self.empty_streak = 0;
                    let rows = self.apply_transforms(output)?;
                    self.records_processed += rows.len() as u64;
                    self.publish_status_throttled().await;
                    for row in rows {
                        self.sink.write(row).await?;
                    }
                }
                Ok(PollResult::SchemaChange(event)) => {
                    // Schema evolution: the sink flushes its old-schema buffer
                    // and applies the DDL before any new-shape row is written.
                    tracing::info!(
                        "Task {} schema change on table '{}' ({} changes): {:?}",
                        self.context.task_id,
                        event.table,
                        event.changes.len(),
                        event.statement
                    );
                    self.sink.apply_schema_change(&event).await?;
                    if let Some(schema) = &mut self.output_schema {
                        if let Err(e) = schema.apply_schema_change_event(&event) {
                            anyhow::bail!("apply schema change to output schema: {}", e);
                        }
                    }
                }
                Ok(PollResult::Empty) => {
                    // Give buffering sinks a chance to flush tail records
                    // (linger-based flush), then back off adaptively.
                    self.sink.poll_flush().await?;
                    match self.empty_streak {
                        0 => tokio::task::yield_now().await,
                        n => {
                            let delay =
                                IDLE_BACKOFF_MS[(n as usize - 1).min(IDLE_BACKOFF_MS.len() - 1)];
                            tokio::time::sleep(Duration::from_millis(delay)).await;
                        }
                    }
                    self.empty_streak = self.empty_streak.saturating_add(1);
                }
                Ok(PollResult::EOF) => break,
                Err(e) => {
                    terminal_state = TaskState::Failed {
                        error: e.to_string(),
                    };
                    tracing::error!("Task {} failed in poll loop: {}", self.context.task_id, e);
                    break;
                }
            }
        }

        if terminal_state == TaskState::Completed || terminal_state == TaskState::Cancelled {
            // Exit barrier: final flush + durable snapshot of this task's
            // states so a restart resumes where the job stopped. Without a
            // coordinator handle this degenerates to a plain final flush.
            if self.context.checkpoint_handle.is_some() {
                if let Err(e) = self.execute_barrier(FINAL_CHECKPOINT_ID).await {
                    tracing::error!(
                        "Task {} exit barrier failed: {}",
                        self.context.task_id,
                        e
                    );
                    if terminal_state == TaskState::Completed {
                        terminal_state = TaskState::Failed {
                            error: format!("exit barrier failed: {}", e),
                        };
                    }
                }
            } else if let Err(e) = self.sink.prepare_commit(FINAL_CHECKPOINT_ID).await {
                tracing::error!(
                    "Task {} final prepare_commit failed: {}",
                    self.context.task_id,
                    e
                );
                if terminal_state == TaskState::Completed {
                    terminal_state = TaskState::Failed {
                        error: format!("final sink flush failed: {}", e),
                    };
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
        // Tell the coordinator this task will not process more triggers.
        if let Some(handle) = &self.context.checkpoint_handle {
            handle.report(TaskToDriver::Done {
                task_id: self.context.task_id.clone(),
            });
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

    /// Publish the processed-records counter to the shared status at most
    /// every [`STATUS_PUBLISH_INTERVAL`]; the exact final count is written
    /// once at task exit.
    async fn publish_status_throttled(&mut self) {
        let now = Instant::now();
        let due = match self.last_status_publish {
            Some(last) => now.duration_since(last) >= STATUS_PUBLISH_INTERVAL,
            None => true,
        };
        if !due {
            return;
        }
        self.last_status_publish = Some(now);
        let records = self.records_processed;
        self.status.lock().await.processed_records = records;
    }

    /// Trigger a checkpoint when the configured interval has elapsed.
    /// Returns the checkpoint id on success.
    async fn maybe_trigger_checkpoint(&mut self) -> anyhow::Result<Option<u64>> {
        // Coordinator-driven gates own checkpointing when present; the
        // interval path is the cluster worker's listener protocol.
        if self.context.checkpoint_listener.is_none()
            || self.context.checkpoint_handle.is_some()
        {
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
        self.sink.prepare_commit(cp_id).await?;

        // 2. Capture the source state after the flush.
        let state = self
            .reader
            .snapshot_state()
            .await
            .map_err(|e| anyhow::anyhow!("reader snapshot_state failed: {}", e))?;

        // 3. Persist + report via the listener.
        if let Some(listener) = &self.context.checkpoint_listener {
            listener
                .on_checkpoint(
                    &self.context.job_id,
                    &self.context.task_id,
                    cp_id,
                    now,
                    state,
                )
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

    /// Execute one coordinator-driven barrier cut (Java: barrier received
    /// from the source, aligned through the chain). Phases:
    /// 1. `sink.prepare_commit(cp)` — 2PC phase 1, flush + commit descriptor
    /// 2. `sink.snapshot_state()` — writer state for restore
    /// 3. `reader.snapshot_state()` — source position AFTER the flush, so
    ///    a restart from this checkpoint replays a superset of what was
    ///    committed, never less
    /// 4. report all three payloads to the checkpoint driver
    async fn execute_barrier(&mut self, checkpoint_id: u64) -> anyhow::Result<()> {
        let commit_infos = self.sink.prepare_commit(checkpoint_id).await?;
        let writer_state = self.sink.snapshot_state().await?;
        let reader_state = self
            .reader
            .snapshot_state()
            .await
            .map_err(|e| anyhow::anyhow!("reader snapshot_state failed: {}", e))?;
        self.checkpoints_completed += 1;
        self.last_checkpoint_at = Some(crate::now_millis());
        self.last_commit_infos = commit_infos.clone();
        if let Some(handle) = self.context.checkpoint_handle.clone() {
            handle.report(TaskToDriver::Checkpoint(
                crate::local_checkpoint::TaskCheckpointReport {
                    task_id: self.context.task_id.clone(),
                    checkpoint_id,
                    pipeline: self.context.stage_id.clone(),
                    subtask: self.context.subtask_index,
                    parallelism: self.context.parallelism,
                    reader_state,
                    writer_state,
                    commit_infos,
                },
            ));
        }
        tracing::debug!(
            "Task {} barrier {} done (records={})",
            self.context.task_id,
            checkpoint_id,
            self.records_processed
        );
        Ok(())
    }

    /// Handle a checkpoint resolution: phase 2 on completion (committer
    /// commit + reader offset commit), abort on failure.
    async fn handle_checkpoint_event(&mut self, event: CheckpointEvent) -> anyhow::Result<()> {
        match event {
            CheckpointEvent::Completed(checkpoint_id) => {
                if let Some(committer) = &mut self.committer {
                    if !self.last_commit_infos.is_empty() {
                        let infos = self.last_commit_infos.clone();
                        let aggregated = committer.commit(infos).await?;
                        tracing::debug!(
                            "Task {} checkpoint {} phase 2 committed: {:?}",
                            self.context.task_id,
                            checkpoint_id,
                            aggregated
                        );
                    }
                }
                self.last_commit_infos.clear();
                let handle = self.context.checkpoint_handle.clone();
                self.reader
                    .notify_checkpoint_complete(checkpoint_id)
                    .await
                    .map_err(|e| anyhow::anyhow!("notify_checkpoint_complete failed: {}", e))?;
                if let Some(handle) = handle {
                    handle.report(TaskToDriver::CommitDone {
                        task_id: self.context.task_id.clone(),
                        checkpoint_id,
                    });
                }
            }
            CheckpointEvent::Aborted(checkpoint_id) => {
                if let Some(committer) = &mut self.committer {
                    if !self.last_commit_infos.is_empty() {
                        let infos = self.last_commit_infos.clone();
                        committer.abort(infos).await?;
                    }
                }
                self.last_commit_infos.clear();
                tracing::warn!(
                    "Task {} checkpoint {} aborted (committers rolled back)",
                    self.context.task_id,
                    checkpoint_id
                );
            }
        }
        Ok(())
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
