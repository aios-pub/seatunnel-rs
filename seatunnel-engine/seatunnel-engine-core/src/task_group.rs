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
use crate::connector_factory::{
    BoxedSinkCommitter, BoxedSinkWriter, BoxedSourceReader, BoxedTransform,
};
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

/// Sample one DATA log line every N processed records so the console can
/// show live data flow without flooding the task log ring.
const DATA_LOG_SAMPLE: u64 = 100;

/// Interval between periodic pipeline stats lines (throughput + cumulative
/// per-stage times). The final summary is emitted once at task exit.
const STATS_LOG_INTERVAL: Duration = Duration::from_secs(30);

/// Cumulative per-stage timings accumulated over the run loop; surfaced by
/// the periodic stats line and the final task summary.
#[derive(Default)]
struct StageTimes {
    /// Time spent inside productive `reader.poll_next()` calls (empty/EOF
    /// polls are not counted so idle time does not skew the breakdown).
    source: Duration,
    /// Time spent running the transform chain.
    transform: Duration,
    /// Time spent writing rows into the sink.
    sink: Duration,
    /// Number of record batches pulled from the source.
    batches: u64,
}

/// Compact `f0=.., f1=..` rendering of a row for DATA log lines.
fn row_summary(row: &seatunnel_api::Row) -> String {
    let mut parts = Vec::new();
    for i in 0..row.field_count() {
        parts.push(format!("f{}={:?}", i, row.get(i)));
    }
    let joined = parts.join(", ");
    if joined.len() > 200 {
        format!("{}…", &joined[..200])
    } else {
        joined
    }
}

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
    /// Epoch-ms of the most recent record; published with the throttled
    /// status update so heartbeats can report task liveness.
    last_record_at: i64,
    /// (id, state size) of the most recent completed checkpoint.
    last_checkpoint_meta: (u64, u64),
    /// Shared sink-metrics handle (same `Arc` the writer received via
    /// `SinkWriterContext`); snapshotted into the published status.
    sink_metrics: Option<std::sync::Arc<seatunnel_api::sink::SinkMetrics>>,
    /// Bounded task log ring surfaced through the worker heartbeat.
    logs: crate::task_log::TaskLogRing,
    /// Per-stage timing accumulators for the stats/summary log lines.
    stage_times: StageTimes,
    /// Instant of the last periodic stats line (throttle state).
    last_stats_log: Option<Instant>,
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
            last_record_at: 0,
            last_checkpoint_meta: (0, 0),
            sink_metrics: None,
            logs: crate::task_log::TaskLogRing::default(),
            stage_times: StageTimes::default(),
            last_stats_log: None,
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

    /// Attach the sink-metrics handle the writer was created with (same
    /// `Arc` that went into `SinkWriterContext`), so status publishes can
    /// snapshot it. Mirrors the `attach_task_metrics` wiring pattern.
    pub fn with_sink_metrics(
        mut self,
        metrics: std::sync::Arc<seatunnel_api::sink::SinkMetrics>,
    ) -> Self {
        self.sink_metrics = Some(metrics);
        self
    }

    pub fn with_output_schema(mut self, schema: TableSchema) -> Self {
        self.output_schema = Some(schema);
        self
    }

    pub fn status(&self) -> Arc<tokio::sync::Mutex<TaskStatus>> {
        self.status.clone()
    }

    /// Task log ring, shared with the worker for heartbeat shipping.
    pub fn logs(&self) -> &crate::task_log::TaskLogRing {
        &self.logs
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
        let started = Instant::now();
        self.last_stats_log = Some(started);
        self.logs.info(format!(
            "task started (job={}, parallelism={})",
            self.context.job_id, self.context.parallelism
        ));

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
                self.logs.info(format!(
                    "checkpoint #{} completed (records={})",
                    cp_id, self.records_processed
                ));
            }

            let poll_started = Instant::now();
            match self.reader.poll_next().await {
                Ok(PollResult::Record(output)) => {
                    self.empty_streak = 0;
                    let source_elapsed = poll_started.elapsed();
                    let transform_started = Instant::now();
                    let rows = self.apply_transforms(output)?;
                    let transform_elapsed = transform_started.elapsed();
                    self.records_processed += rows.len() as u64;
                    self.last_record_at = crate::now_millis();
                    // Sample one row per DATA_LOG_SAMPLE records so the
                    // console can show live data without flooding the ring.
                    if self.records_processed % DATA_LOG_SAMPLE == 0
                        || self.records_processed == rows.len() as u64
                    {
                        if let Some(row) = rows.last() {
                            self.logs.push(
                                "DATA",
                                format!("record #{}: {}", self.records_processed, row_summary(row)),
                            );
                        }
                    }
                    self.publish_status_throttled().await;
                    // Capture debug-only details before the rows are moved
                    // into the sink write loop.
                    let batch_rows = rows.len();
                    let debug_last_row = if tracing::enabled!(tracing::Level::DEBUG) {
                        rows.last().map(row_summary)
                    } else {
                        None
                    };
                    let sink_started = Instant::now();
                    for row in rows {
                        self.sink.write(row).await?;
                    }
                    let sink_elapsed = sink_started.elapsed();
                    self.stage_times.source += source_elapsed;
                    self.stage_times.transform += transform_elapsed;
                    self.stage_times.sink += sink_elapsed;
                    self.stage_times.batches += 1;
                    // Debug mode prints every batch with its per-stage cost.
                    if let Some(last_row) = debug_last_row {
                        tracing::debug!(
                            "Task {} batch: rows={} batch={}us source={}us transform={}us sink={}us last: {}",
                            self.context.task_id,
                            batch_rows,
                            (source_elapsed + transform_elapsed + sink_elapsed).as_micros(),
                            source_elapsed.as_micros(),
                            transform_elapsed.as_micros(),
                            sink_elapsed.as_micros(),
                            last_row,
                        );
                    }
                    self.maybe_log_stats(started);
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
                    self.logs.info(format!(
                        "schema change on '{}': {}",
                        event.table,
                        event.statement.as_deref().unwrap_or("<none>")
                    ));
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
                Ok(PollResult::EOF) => {
                    self.logs.info("source reached EOF");
                    break;
                }
                Err(e) => {
                    terminal_state = TaskState::Failed {
                        error: e.to_string(),
                    };
                    self.logs.error(format!("poll loop failed: {}", e));
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
                    tracing::error!("Task {} exit barrier failed: {}", self.context.task_id, e);
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
            status.state = terminal_state.clone();
            status.end_time = crate::now_millis();
            status.processed_records = self.records_processed;
            status.last_record_at = self.last_record_at;
            status.last_checkpoint_id = self.last_checkpoint_meta.0;
            status.last_checkpoint_size = self.last_checkpoint_meta.1;
        }
        self.logs.info(format!(
            "task finished: state={:?} records={} checkpoints={}",
            terminal_state, self.records_processed, self.checkpoints_completed
        ));

        tracing::info!(
            "Task {} finished: state={} records={} checkpoints={}",
            self.context.task_id,
            self.status.lock().await.state,
            self.records_processed,
            self.checkpoints_completed
        );

        // Whole-pipeline timing summary: wall clock, effective throughput
        // and the share of the wall time each stage consumed. Stage times
        // are sequential segments of the loop, so they sum to <= elapsed
        // (the rest is idle backoff / checkpoint pauses).
        let total_elapsed = started.elapsed();
        let secs = total_elapsed.as_secs_f64();
        let rate = if secs > 0.0 {
            self.records_processed as f64 / secs
        } else {
            0.0
        };
        let share = |d: Duration| {
            if secs > 0.0 {
                d.as_secs_f64() / secs * 100.0
            } else {
                0.0
            }
        };
        let summary = format!(
            "summary: records={} batches={} elapsed={:.3}s throughput={:.1}/s | source={}ms ({:.1}%) transform={}ms ({:.1}%) sink={}ms ({:.1}%)",
            self.records_processed,
            self.stage_times.batches,
            secs,
            rate,
            self.stage_times.source.as_millis(),
            share(self.stage_times.source),
            self.stage_times.transform.as_millis(),
            share(self.stage_times.transform),
            self.stage_times.sink.as_millis(),
            share(self.stage_times.sink),
        );
        self.logs.info(summary.clone());
        tracing::info!("Task {} {}", self.context.task_id, summary);

        Ok(self.status.lock().await.clone())
    }

    /// Emit the periodic pipeline stats line, throttled to
    /// [`STATS_LOG_INTERVAL`]: processed records, effective throughput and
    /// the cumulative per-stage times since the task started.
    fn maybe_log_stats(&mut self, started: Instant) {
        let now = Instant::now();
        let due = match self.last_stats_log {
            Some(last) => now.duration_since(last) >= STATS_LOG_INTERVAL,
            None => true,
        };
        if !due {
            return;
        }
        self.last_stats_log = Some(now);
        let secs = now.duration_since(started).as_secs_f64();
        let rate = if secs > 0.0 {
            self.records_processed as f64 / secs
        } else {
            0.0
        };
        let line = format!(
            "stats: records={} elapsed={:.1}s rate={:.1}/s source={}ms transform={}ms sink={}ms batches={}",
            self.records_processed,
            secs,
            rate,
            self.stage_times.source.as_millis(),
            self.stage_times.transform.as_millis(),
            self.stage_times.sink.as_millis(),
            self.stage_times.batches,
        );
        self.logs.info(line.clone());
        tracing::info!("Task {} {}", self.context.task_id, line);
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
        let last_record_at = self.last_record_at;
        let checkpoint_meta = self.last_checkpoint_meta;
        // Windowed sink snapshot; stay `None` until the sink has actually
        // recorded something (unused sinks never surface zero noise).
        let sink_metrics = self.sink_metrics.as_ref().map(|metrics| {
            let snapshot = metrics.snapshot();
            (snapshot.sent > 0
                || snapshot.failed > 0
                || snapshot.in_flight > 0
                || snapshot.last_error.is_some())
            .then_some(snapshot)
        });
        let mut status = self.status.lock().await;
        status.processed_records = records;
        status.last_record_at = last_record_at;
        status.last_checkpoint_id = checkpoint_meta.0;
        status.last_checkpoint_size = checkpoint_meta.1;
        status.sink_metrics = sink_metrics.flatten();
    }

    /// Trigger a checkpoint when the configured interval has elapsed.
    /// Returns the checkpoint id on success.
    async fn maybe_trigger_checkpoint(&mut self) -> anyhow::Result<Option<u64>> {
        // Coordinator-driven gates own checkpointing when present; the
        // interval path is the cluster worker's listener protocol.
        if self.context.checkpoint_listener.is_none() || self.context.checkpoint_handle.is_some() {
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
        let prepare_started = Instant::now();
        self.sink.prepare_commit(cp_id).await?;
        let prepare_elapsed = prepare_started.elapsed();

        // 2. Capture the source state after the flush.
        let snapshot_started = Instant::now();
        let state = self
            .reader
            .snapshot_state()
            .await
            .map_err(|e| anyhow::anyhow!("reader snapshot_state failed: {}", e))?;
        let snapshot_elapsed = snapshot_started.elapsed();
        self.last_checkpoint_meta = (cp_id, state.len() as u64);

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
            "Task {} checkpoint {} completed (records={}, sink prepare_commit={}ms, reader snapshot={}ms)",
            self.context.task_id,
            cp_id,
            self.records_processed,
            prepare_elapsed.as_millis(),
            snapshot_elapsed.as_millis()
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
        let barrier_started = Instant::now();
        let prepare_started = Instant::now();
        let commit_infos = self.sink.prepare_commit(checkpoint_id).await?;
        let prepare_elapsed = prepare_started.elapsed();
        let writer_state = self.sink.snapshot_state().await?;
        let reader_started = Instant::now();
        let reader_state = self
            .reader
            .snapshot_state()
            .await
            .map_err(|e| anyhow::anyhow!("reader snapshot_state failed: {}", e))?;
        let reader_elapsed = reader_started.elapsed();
        self.checkpoints_completed += 1;
        self.last_checkpoint_at = Some(crate::now_millis());
        // The exit barrier (FINAL_CHECKPOINT_ID) is a durable flush for
        // restart, not a progress checkpoint: the progress view keeps the
        // last coordinated id (the FINAL marker does not fit the i64
        // heartbeat field and would zero it out after clamping).
        if checkpoint_id != FINAL_CHECKPOINT_ID {
            self.last_checkpoint_meta = (checkpoint_id, reader_state.len() as u64);
        }
        self.last_commit_infos = commit_infos.clone();
        // Captured before the payloads are moved into the driver report.
        let commit_infos_len = commit_infos.len();
        let reader_state_len = reader_state.len();
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
        tracing::info!(
            "Task {} barrier {}: total={}ms (sink prepare_commit={}ms, {} commit infos; reader snapshot={}ms, {} bytes; records={})",
            self.context.task_id,
            checkpoint_id,
            barrier_started.elapsed().as_millis(),
            prepare_elapsed.as_millis(),
            commit_infos_len,
            reader_elapsed.as_millis(),
            reader_state_len,
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
                        let commit_started = Instant::now();
                        let aggregated = committer.commit(infos).await?;
                        tracing::info!(
                            "Task {} checkpoint {} phase 2 committed in {}ms: {:?}",
                            self.context.task_id,
                            checkpoint_id,
                            commit_started.elapsed().as_millis(),
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
