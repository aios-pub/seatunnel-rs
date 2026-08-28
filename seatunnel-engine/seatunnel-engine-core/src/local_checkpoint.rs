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

//! Local-mode checkpoint coordinator: an in-process port of Java Zeta's
//! `CheckpointCoordinator` flow, specialized for single-process execution
//! where every pipeline is a self-contained `source → transforms → sink`
//! chain (no inter-task data flow, so "barrier alignment" degenerates to
//! "every task cut at its next safe point").
//!
//! Protocol per checkpoint N (all ids are global and monotonic):
//!   1. driver triggers N on every live task gate (Java:
//!      `CheckpointBarrierTriggerOperation` to starting subtasks)
//!   2. each `TaskGroup` executes the barrier cut between polls:
//!      `sink.prepare_commit(N)` → `sink.snapshot_state()` →
//!      `reader.snapshot_state()` and reports the three payloads
//!   3. driver aggregates all reports (Java: task acknowledges), writes
//!      one durable `CheckpointEnvelope` (atomic tmp+fsync+rename) and
//!      prunes old checkpoints by retention
//!   4. driver broadcasts `Completed(N)`; tasks then run 2PC phase 2
//!      (`SinkCommitter::commit`) and `SourceReader::notify_checkpoint_complete`
//!      (Java: `CheckpointFinishedOperation` / notifyCheckpointComplete)
//!   5. a timeout or a task-side failure broadcasts `Aborted(N)`: committers
//!      abort, nothing is persisted, and the next interval retries with N+1
//!
//! Restart recovery (Java: restore from latest `CompletedCheckpoint`):
//! `LocalCheckpointStore::load_latest` returns the newest envelope; the
//! runner re-creates readers (existing `create_source(.., restore_state)`)
//! and sink writers from the per-task states and the driver continues at
//! `checkpoint_id + 1`.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use tokio::sync::{Notify, mpsc};

use crate::now_millis;

/// Default checkpoint interval (matches the cluster default).
pub const DEFAULT_CHECKPOINT_INTERVAL_MS: u64 = 30_000;
/// Default time a checkpoint may wait for all task reports (Java default).
pub const DEFAULT_CHECKPOINT_TIMEOUT_MS: u64 = 30_000;
/// Default number of completed checkpoints kept on disk.
pub const DEFAULT_KEEP_CHECKPOINT_COUNT: usize = 3;

/// Checkpoint id used for the flush a task performs on exit. It is never
/// persisted as an envelope id — envelopes always use the global counter.
pub use crate::task_group::FINAL_CHECKPOINT_ID;

// ---------------------------------------------------------------------------
// Gate: coordinator ↔ task signaling
// ---------------------------------------------------------------------------

/// Event delivered to a task after a checkpoint resolution.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CheckpointEvent {
    /// Checkpoint durably persisted; run 2PC phase 2 now.
    Completed(u64),
    /// Checkpoint failed/timed out; abort prepared commits.
    Aborted(u64),
}

struct GateShared {
    /// Coordinator → task: barrier trigger for this checkpoint id.
    pending_trigger: std::sync::Mutex<Option<u64>>,
    /// Coordinator → task: completion events, in order.
    pending_events: std::sync::Mutex<Vec<CheckpointEvent>>,
    notify: Notify,
    closed: std::sync::atomic::AtomicBool,
}

/// Per-task handle given to the `TaskGroup` via `TaskContext`.
///
/// All methods are cheap; `take_trigger`/`poll_event` never block so the
/// record loop pays nothing while no checkpoint is in flight.
#[derive(Clone)]
pub struct CheckpointHandle {
    shared: Arc<GateShared>,
    report_tx: mpsc::UnboundedSender<TaskToDriver>,
}

impl CheckpointHandle {
    /// Take the pending barrier trigger, if any.
    pub fn take_trigger(&self) -> Option<u64> {
        self.shared.pending_trigger.lock().unwrap().take()
    }

    /// Take the next pending completion event, if any.
    pub fn poll_event(&self) -> Option<CheckpointEvent> {
        let mut events = self.shared.pending_events.lock().unwrap();
        if events.is_empty() {
            None
        } else {
            Some(events.remove(0))
        }
    }

    /// Wait until a trigger or event arrives (used by tests and idle tasks
    /// that prefer parking over backoff).
    pub async fn wait_for_signal(&self) {
        self.shared.notify.notified().await
    }

    /// Report a barrier cut result to the coordinator.
    pub fn report(&self, message: TaskToDriver) {
        let _ = self.report_tx.send(message);
    }

    /// Whether the coordinator side is still alive.
    pub fn coordinator_alive(&self) -> bool {
        !self
            .shared
            .closed
            .load(std::sync::atomic::Ordering::Acquire)
    }
}

/// Control side of one task's gate (driver-owned).
pub struct GateControl {
    shared: Arc<GateShared>,
}

impl GateControl {
    /// Fire the barrier trigger for `checkpoint_id` on the task.
    pub fn trigger(&self, checkpoint_id: u64) {
        *self.shared.pending_trigger.lock().unwrap() = Some(checkpoint_id);
        self.shared.notify.notify_waiters();
    }

    /// Deliver a completion/abort event to the task.
    pub fn send_event(&self, event: CheckpointEvent) {
        self.shared.pending_events.lock().unwrap().push(event);
        self.shared.notify.notify_waiters();
    }

    /// Mark the coordinator side as gone; the task can decide to stop
    /// waiting for further triggers/events.
    pub fn close(&self) {
        self.shared
            .closed
            .store(true, std::sync::atomic::Ordering::Release);
    }
}

/// Create a standalone (task handle, driver control) pair whose task
/// reports flow to `report_tx`.
///
/// The cluster worker uses this: its tasks' real driver is the remote
/// master, so the worker drains the channel and forwards reports over
/// gRPC while applying triggers and completion events arriving via
/// heartbeats — the same protocol the in-process `LocalCheckpointDriver`
/// speaks, with the driver on the other side of the wire.
pub fn task_gate(
    report_tx: mpsc::UnboundedSender<TaskToDriver>,
) -> (CheckpointHandle, GateControl) {
    let shared = Arc::new(GateShared {
        pending_trigger: std::sync::Mutex::new(None),
        pending_events: std::sync::Mutex::new(Vec::new()),
        notify: Notify::new(),
        closed: std::sync::atomic::AtomicBool::new(false),
    });
    (
        CheckpointHandle {
            shared: Arc::clone(&shared),
            report_tx,
        },
        GateControl { shared },
    )
}

// ---------------------------------------------------------------------------
// Messages and envelope
// ---------------------------------------------------------------------------

/// Messages a task sends to the coordinator.
#[derive(Debug)]
pub enum TaskToDriver {
    /// Barrier cut finished for `checkpoint_id` (phase 1 done).
    Checkpoint(TaskCheckpointReport),
    /// Phase 2 (committer commit + reader notify) finished.
    CommitDone { task_id: String, checkpoint_id: u64 },
    /// The barrier cut failed; abort the pending checkpoint.
    CheckpointFailed {
        task_id: String,
        checkpoint_id: u64,
        error: String,
    },
    /// Task reached a terminal state and will not process more triggers.
    Done { task_id: String },
}

/// One task's phase-1 payloads for a checkpoint.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct TaskCheckpointReport {
    pub task_id: String,
    pub checkpoint_id: u64,
    /// Pipeline name (stable across restarts; used for state mapping).
    pub pipeline: String,
    pub subtask: usize,
    pub parallelism: usize,
    pub reader_state: Vec<u8>,
    pub writer_state: Vec<u8>,
    pub commit_infos: Vec<Vec<u8>>,
}

/// Durable, whole-job checkpoint: all tasks' states for one barrier.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct CheckpointEnvelope {
    pub job_id: String,
    pub checkpoint_id: u64,
    pub timestamp: i64,
    /// True when written during graceful shutdown (Java savepoint flavor).
    pub is_final: bool,
    pub tasks: Vec<TaskCheckpointReport>,
}

impl CheckpointEnvelope {
    /// Look up a task's state by (pipeline, subtask).
    pub fn task_state(&self, pipeline: &str, subtask: usize) -> Option<&TaskCheckpointReport> {
        self.tasks
            .iter()
            .find(|t| t.pipeline == pipeline && t.subtask == subtask)
    }
}

// ---------------------------------------------------------------------------
// Durable store
// ---------------------------------------------------------------------------

/// File-backed checkpoint storage for local-mode jobs.
///
/// Layout: `<root>/<job_id>/checkpoint-<id>.json`, written atomically
/// (tmp file + fsync + rename + parent-dir fsync) so a crash never leaves
/// a torn checkpoint behind.
#[derive(Debug, Clone)]
pub struct LocalCheckpointStore {
    root: PathBuf,
}

impl LocalCheckpointStore {
    pub fn new(root: impl AsRef<Path>) -> Self {
        LocalCheckpointStore {
            root: root.as_ref().to_path_buf(),
        }
    }

    fn job_dir(&self, job_id: &str) -> PathBuf {
        // Job ids are operator-supplied; keep them filesystem-safe.
        let safe: String = job_id
            .chars()
            .map(|c| {
                if c.is_alphanumeric() || c == '-' || c == '_' || c == '.' {
                    c
                } else {
                    '_'
                }
            })
            .collect();
        self.root.join(safe)
    }

    /// Durably save an envelope, then prune old checkpoints.
    pub fn save(&self, envelope: &CheckpointEnvelope) -> anyhow::Result<()> {
        let dir = self.job_dir(&envelope.job_id);
        std::fs::create_dir_all(&dir)?;
        let bytes = serde_json::to_vec_pretty(envelope)?;
        let final_path = dir.join(format!("checkpoint-{}.json", envelope.checkpoint_id));
        let tmp_path = dir.join(format!(
            ".checkpoint-{}.{}.tmp",
            envelope.checkpoint_id,
            std::process::id()
        ));
        {
            use std::io::Write;
            let mut file = std::fs::File::create(&tmp_path)?;
            file.write_all(&bytes)?;
            file.sync_all()?;
        }
        std::fs::rename(&tmp_path, &final_path)?;
        // fsync the directory so the rename itself survives a crash.
        if let Ok(dir_file) = std::fs::File::open(&dir) {
            let _ = dir_file.sync_all();
        }
        Ok(())
    }

    /// Load the newest checkpoint for a job (highest id).
    pub fn load_latest(&self, job_id: &str) -> anyhow::Result<Option<CheckpointEnvelope>> {
        let mut latest: Option<(u64, CheckpointEnvelope)> = None;
        for envelope in self.load_all(job_id)? {
            if latest
                .as_ref()
                .map(|(id, _)| envelope.checkpoint_id > *id)
                .unwrap_or(true)
            {
                latest = Some((envelope.checkpoint_id, envelope));
            }
        }
        Ok(latest.map(|(_, envelope)| envelope))
    }

    /// Load every checkpoint for a job, oldest first.
    pub fn load_all(&self, job_id: &str) -> anyhow::Result<Vec<CheckpointEnvelope>> {
        let dir = self.job_dir(job_id);
        let mut ids = Vec::new();
        if let Ok(entries) = std::fs::read_dir(&dir) {
            for entry in entries.flatten() {
                let name = entry.file_name();
                let name = name.to_string_lossy();
                if let Some(id) = name
                    .strip_prefix("checkpoint-")
                    .and_then(|s| s.strip_suffix(".json"))
                    .and_then(|s| s.parse::<u64>().ok())
                {
                    ids.push(id);
                }
            }
        }
        ids.sort_unstable();
        let mut envelopes = Vec::with_capacity(ids.len());
        for id in ids {
            let path = dir.join(format!("checkpoint-{}.json", id));
            if let Ok(bytes) = std::fs::read(&path) {
                if let Ok(envelope) = serde_json::from_slice(&bytes) {
                    envelopes.push(envelope);
                }
            }
        }
        Ok(envelopes)
    }

    /// Keep only the newest `keep` checkpoints for a job.
    pub fn prune(&self, job_id: &str, keep: usize) -> anyhow::Result<()> {
        let envelopes = self.load_all(job_id)?;
        if envelopes.len() > keep {
            let dir = self.job_dir(job_id);
            for envelope in &envelopes[..envelopes.len() - keep] {
                let path = dir.join(format!("checkpoint-{}.json", envelope.checkpoint_id));
                let _ = std::fs::remove_file(&path);
            }
        }
        Ok(())
    }

    /// Remove all state for a job.
    pub fn drop_job(&self, job_id: &str) -> anyhow::Result<()> {
        let dir = self.job_dir(job_id);
        if dir.exists() {
            std::fs::remove_dir_all(&dir)?;
        }
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Plan (builder) + driver
// ---------------------------------------------------------------------------

/// Static description of one task, supplied at registration time.
#[derive(Debug, Clone)]
pub struct TaskRegistration {
    pub task_id: String,
    pub pipeline: String,
    pub subtask: usize,
    pub parallelism: usize,
}

struct TaskEntry {
    meta: TaskRegistration,
    control: Arc<GateControl>,
    alive: bool,
}

/// Builder collecting task registrations before the driver starts; also
/// the single place that knows the restore context.
pub struct LocalCheckpointPlan {
    job_id: String,
    store: LocalCheckpointStore,
    interval: Duration,
    timeout: Duration,
    keep: usize,
    restore_from: Option<CheckpointEnvelope>,
    entries: Vec<TaskEntry>,
    report_tx: mpsc::UnboundedSender<TaskToDriver>,
    report_rx: mpsc::UnboundedReceiver<TaskToDriver>,
}

impl LocalCheckpointPlan {
    pub fn new(
        state_root: impl AsRef<Path>,
        job_id: impl Into<String>,
        interval: Duration,
    ) -> Self {
        let (report_tx, report_rx) = mpsc::unbounded_channel();
        LocalCheckpointPlan {
            job_id: job_id.into(),
            store: LocalCheckpointStore::new(state_root),
            interval,
            timeout: Duration::from_millis(DEFAULT_CHECKPOINT_TIMEOUT_MS),
            keep: DEFAULT_KEEP_CHECKPOINT_COUNT,
            restore_from: None,
            entries: Vec::new(),
            report_tx,
            report_rx,
        }
    }

    pub fn with_timeout(mut self, timeout: Duration) -> Self {
        self.timeout = timeout;
        self
    }

    pub fn with_keep_count(mut self, keep: usize) -> Self {
        self.keep = keep.max(1);
        self
    }

    /// Load the newest checkpoint for the job so the runner can restore
    /// tasks and the driver can continue the id sequence.
    pub fn restore_from_latest(mut self) -> anyhow::Result<Self> {
        self.restore_from = self.store.load_latest(&self.job_id)?;
        Ok(self)
    }

    /// The envelope the driver will restore from, if any.
    pub fn restore_envelope(&self) -> Option<&CheckpointEnvelope> {
        self.restore_from.as_ref()
    }

    /// Register a task and get the handle it polls in its run loop.
    pub fn register(&mut self, meta: TaskRegistration) -> CheckpointHandle {
        let shared = Arc::new(GateShared {
            pending_trigger: std::sync::Mutex::new(None),
            pending_events: std::sync::Mutex::new(Vec::new()),
            notify: Notify::new(),
            closed: std::sync::atomic::AtomicBool::new(false),
        });
        let handle = CheckpointHandle {
            shared: Arc::clone(&shared),
            report_tx: self.report_tx.clone(),
        };
        self.entries.push(TaskEntry {
            meta,
            control: Arc::new(GateControl { shared }),
            alive: true,
        });
        handle
    }

    /// Number of registered tasks (diagnostics).
    pub fn task_count(&self) -> usize {
        self.entries.len()
    }

    /// Consume the plan and produce the runnable driver.
    pub fn build(mut self) -> LocalCheckpointDriver {
        // The next id continues the sequence after the restored checkpoint;
        // ids must stay strictly increasing across restarts so envelope
        // files never collide and sinks see monotonic checkpoints.
        let next_checkpoint_id = self
            .restore_from
            .as_ref()
            .map(|e| e.checkpoint_id.saturating_add(1))
            .unwrap_or(1);
        LocalCheckpointDriver {
            job_id: self.job_id,
            interval: self.interval,
            timeout: self.timeout,
            keep: self.keep,
            store: self.store,
            next_checkpoint_id,
            completed_checkpoints: 0,
            entries: std::mem::take(&mut self.entries),
            report_rx: self.report_rx,
            last_envelope: self.restore_from.clone(),
            final_states: HashMap::new(),
        }
    }
}

/// Coordinates checkpoints for one local-mode job. Spawned by the runner;
/// exits when every task is done, when cancelled, or on a fatal error.
pub struct LocalCheckpointDriver {
    job_id: String,
    interval: Duration,
    timeout: Duration,
    keep: usize,
    store: LocalCheckpointStore,
    next_checkpoint_id: u64,
    completed_checkpoints: u64,
    entries: Vec<TaskEntry>,
    report_rx: mpsc::UnboundedReceiver<TaskToDriver>,
    last_envelope: Option<CheckpointEnvelope>,
    /// Exit-time barrier reports (`FINAL_CHECKPOINT_ID`); folded into the
    /// next envelope so a finished task's last state is not lost.
    final_states: HashMap<String, TaskCheckpointReport>,
}

/// A checkpoint currently awaiting task reports.
struct PendingCheckpoint {
    checkpoint_id: u64,
    timestamp: i64,
    reports: HashMap<String, TaskCheckpointReport>,
    /// Task ids that still owe a report (or a Done).
    awaiting: Vec<String>,
}

impl LocalCheckpointDriver {
    /// Checkpoints completed since process start (diagnostics).
    pub fn completed_checkpoints(&self) -> u64 {
        self.completed_checkpoints
    }

    /// Run until all tasks finish or `shutdown` fires. On shutdown, a final
    /// checkpoint is taken before tasks are cancelled (savepoint flavor).
    pub async fn run(
        mut self,
        shutdown: tokio_util::sync::CancellationToken,
        task_cancel: Arc<tokio_util::sync::CancellationToken>,
    ) -> anyhow::Result<()> {
        let mut ticker = tokio::time::interval(self.interval);
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        ticker.tick().await; // consume the immediate first tick

        let mut pending: Option<PendingCheckpoint> = None;
        loop {
            let alive_count = self.entries.iter().filter(|e| e.alive).count();
            if alive_count == 0 && pending.is_none() {
                self.persist_exit_states().await?;
                tracing::info!(
                    job = %self.job_id,
                    "checkpoint driver: all tasks finished, stopping (completed={})",
                    self.completed_checkpoints
                );
                break;
            }
            let pending_deadline = pending
                .as_ref()
                .map(|_| tokio::time::Instant::now() + self.timeout);
            tokio::select! {
                _ = ticker.tick() => {
                    if pending.is_none() && self.entries.iter().any(|e| e.alive) {
                        pending = Some(self.trigger_checkpoint().await);
                    }
                }
                message = self.report_rx.recv() => {
                    let Some(message) = message else { break };
                    if self.handle_message(message, &mut pending).await? {
                        break;
                    }
                }
                _ = async {
                    match pending_deadline {
                        Some(deadline) => tokio::time::sleep_until(deadline).await,
                        None => std::future::pending::<()>().await,
                    }
                } => {
                    if let Some(p) = pending.take() {
                        self.abort_checkpoint(p.checkpoint_id, "timed out waiting for task reports");
                    }
                }
                _ = shutdown.cancelled() => {
                    tracing::info!(
                        job = %self.job_id,
                        "checkpoint driver: shutdown signal, taking final checkpoint"
                    );
                    // A half-collected checkpoint is superseded by the final
                    // one below; abort it so tasks unwind phase 1.
                    if let Some(p) = pending.take() {
                        self.abort_checkpoint(p.checkpoint_id, "shutdown during checkpoint");
                    }
                    self.final_checkpoint(&task_cancel).await?;
                    break;
                }
            }
        }
        for entry in &self.entries {
            entry.control.close();
        }
        Ok(())
    }

    /// Fire a barrier on every live task and return the pending checkpoint.
    async fn trigger_checkpoint(&mut self) -> PendingCheckpoint {
        let checkpoint_id = self.next_checkpoint_id;
        self.next_checkpoint_id = self.next_checkpoint_id.saturating_add(1);
        let timestamp = now_millis();
        let mut awaiting = Vec::new();
        for entry in &self.entries {
            if entry.alive {
                awaiting.push(entry.meta.task_id.clone());
                entry.control.trigger(checkpoint_id);
            }
        }
        tracing::info!(
            job = %self.job_id,
            "checkpoint {}: triggered on {} task(s)",
            checkpoint_id,
            awaiting.len()
        );
        PendingCheckpoint {
            checkpoint_id,
            timestamp,
            reports: HashMap::new(),
            awaiting,
        }
    }

    /// Process one task message. Returns `true` when the driver should
    /// shut down (all tasks finished).
    async fn handle_message(
        &mut self,
        message: TaskToDriver,
        pending: &mut Option<PendingCheckpoint>,
    ) -> anyhow::Result<bool> {
        match message {
            TaskToDriver::Checkpoint(report) => {
                if report.checkpoint_id == FINAL_CHECKPOINT_ID {
                    // Exit-time barrier from a task reaching EOF/cancel:
                    // remember it for the next envelope instead of tying it
                    // to the pending barrier id.
                    self.final_states.insert(report.task_id.clone(), report);
                    return Ok(false);
                }
                let Some(p) = pending else { return Ok(false) };
                if p.checkpoint_id != report.checkpoint_id {
                    tracing::warn!(
                        job = %self.job_id,
                        "checkpoint report for {} while pending is {}; ignoring",
                        report.checkpoint_id,
                        p.checkpoint_id
                    );
                    return Ok(false);
                }
                p.awaiting.retain(|id| id != &report.task_id);
                p.reports.insert(report.task_id.clone(), report);
                if p.awaiting.is_empty() {
                    let p = pending.take().unwrap();
                    self.complete_checkpoint(p, false).await?;
                }
            }
            TaskToDriver::CommitDone {
                task_id,
                checkpoint_id,
            } => {
                tracing::debug!(
                    job = %self.job_id,
                    "checkpoint {}: task {} finished phase 2",
                    checkpoint_id,
                    task_id
                );
            }
            TaskToDriver::CheckpointFailed {
                task_id,
                checkpoint_id,
                error,
            } => {
                tracing::error!(
                    job = %self.job_id,
                    "checkpoint {} failed on task {}: {}",
                    checkpoint_id,
                    task_id,
                    error
                );
                if let Some(p) = pending.take() {
                    self.abort_checkpoint(
                        p.checkpoint_id,
                        &format!("task {} reported failure", task_id),
                    );
                }
            }
            TaskToDriver::Done { task_id } => {
                tracing::info!(job = %self.job_id, "checkpoint driver: task {} done", task_id);
                if let Some(entry) = self.entries.iter_mut().find(|e| e.meta.task_id == task_id) {
                    entry.alive = false;
                }
                if let Some(p) = pending.as_mut() {
                    p.awaiting.retain(|id| id != &task_id);
                    if p.awaiting.is_empty() {
                        let p = pending.take().unwrap();
                        self.complete_checkpoint(p, false).await?;
                    }
                }
                if self.entries.iter().all(|e| !e.alive) {
                    return Ok(true);
                }
            }
        }
        Ok(false)
    }

    /// When every task finished (bounded job or full shutdown), persist the
    /// exit states so a restart resumes instead of replaying from scratch
    /// (Java's final COMPLETED checkpoint on `readyToClose`).
    async fn persist_exit_states(&mut self) -> anyhow::Result<()> {
        if self.final_states.is_empty() {
            return Ok(());
        }
        let checkpoint_id = self.next_checkpoint_id;
        self.next_checkpoint_id = self.next_checkpoint_id.saturating_add(1);
        let mut tasks: Vec<TaskCheckpointReport> =
            self.final_states.drain().map(|(_, r)| r).collect();
        for task in &mut tasks {
            task.checkpoint_id = checkpoint_id;
        }
        tasks.sort_by(|a, b| (&a.pipeline, a.subtask).cmp(&(&b.pipeline, b.subtask)));
        let envelope = CheckpointEnvelope {
            job_id: self.job_id.clone(),
            checkpoint_id,
            timestamp: now_millis(),
            is_final: true,
            tasks,
        };
        if let Err(e) = self.store.save(&envelope) {
            tracing::error!(job = %self.job_id, "exit checkpoint persist failed: {}", e);
            return Ok(());
        }
        let _ = self.store.prune(&self.job_id, self.keep);
        self.completed_checkpoints += 1;
        self.last_envelope = Some(envelope);
        Ok(())
    }

    /// Persist the aggregated envelope, notify tasks, prune retention.
    async fn complete_checkpoint(
        &mut self,
        mut pending: PendingCheckpoint,
        is_final: bool,
    ) -> anyhow::Result<()> {
        let checkpoint_id = pending.checkpoint_id;
        let timestamp = pending.timestamp;
        // Tasks that exited mid-checkpoint contributed an exit-time barrier
        // instead of a report for this id; fold those states in.
        for entry in &self.entries {
            if !entry.alive && !pending.reports.contains_key(&entry.meta.task_id) {
                if let Some(mut final_state) = self.final_states.get(&entry.meta.task_id).cloned() {
                    final_state.checkpoint_id = checkpoint_id;
                    pending
                        .reports
                        .insert(entry.meta.task_id.clone(), final_state);
                }
            }
        }
        let mut tasks: Vec<TaskCheckpointReport> = pending.reports.into_values().collect();
        tasks.sort_by(|a, b| (&a.pipeline, a.subtask).cmp(&(&b.pipeline, b.subtask)));
        let envelope = CheckpointEnvelope {
            job_id: self.job_id.clone(),
            checkpoint_id,
            timestamp,
            is_final,
            tasks,
        };
        // Persist BEFORE broadcasting completion: phase 2 commits only run
        // for checkpoints that are already durable (Java ordering).
        if let Err(e) = self.store.save(&envelope) {
            tracing::error!(
                job = %self.job_id,
                "checkpoint {} persist failed: {} — aborting",
                envelope.checkpoint_id,
                e
            );
            self.abort_checkpoint(checkpoint_id, "persist failed");
            return Ok(());
        }
        if let Err(e) = self.store.prune(&self.job_id, self.keep) {
            tracing::warn!(job = %self.job_id, "checkpoint prune failed: {}", e);
        }
        self.completed_checkpoints += 1;
        self.last_envelope = Some(envelope.clone());
        tracing::info!(
            job = %self.job_id,
            "checkpoint {} completed and persisted ({} task state(s), final={})",
            envelope.checkpoint_id,
            envelope.tasks.len(),
            is_final
        );
        for entry in &self.entries {
            if entry.alive || is_final {
                entry
                    .control
                    .send_event(CheckpointEvent::Completed(envelope.checkpoint_id));
            }
        }
        Ok(())
    }

    /// Tell every task to abort the in-flight checkpoint.
    fn abort_checkpoint(&self, checkpoint_id: u64, reason: &str) {
        tracing::warn!(
            job = %self.job_id,
            "checkpoint {} aborted: {}",
            checkpoint_id,
            reason
        );
        for entry in &self.entries {
            if entry.alive {
                entry
                    .control
                    .send_event(CheckpointEvent::Aborted(checkpoint_id));
            }
        }
    }

    /// Graceful shutdown: one last durable checkpoint, wait for phase 2,
    /// then cancel the tasks so their terminal flush is a pure tail flush.
    async fn final_checkpoint(
        &mut self,
        task_cancel: &Arc<tokio_util::sync::CancellationToken>,
    ) -> anyhow::Result<()> {
        if self.entries.iter().any(|e| e.alive) {
            let mut final_pending = self.trigger_checkpoint().await;
            let deadline = tokio::time::Instant::now() + self.timeout;
            while !final_pending.awaiting.is_empty() {
                let remain = deadline.saturating_duration_since(tokio::time::Instant::now());
                if remain.is_zero() {
                    self.abort_checkpoint(final_pending.checkpoint_id, "final checkpoint timeout");
                    final_pending.awaiting.clear();
                    break;
                }
                match tokio::time::timeout(remain, self.report_rx.recv()).await {
                    Ok(Some(TaskToDriver::Checkpoint(report))) => {
                        if report.checkpoint_id == final_pending.checkpoint_id {
                            final_pending.awaiting.retain(|id| id != &report.task_id);
                            final_pending.reports.insert(report.task_id.clone(), report);
                        } else if report.checkpoint_id == FINAL_CHECKPOINT_ID {
                            self.final_states.insert(report.task_id.clone(), report);
                        }
                    }
                    Ok(Some(TaskToDriver::Done { task_id })) => {
                        final_pending.awaiting.retain(|id| id != &task_id);
                        if let Some(entry) =
                            self.entries.iter_mut().find(|e| e.meta.task_id == task_id)
                        {
                            entry.alive = false;
                        }
                    }
                    Ok(Some(TaskToDriver::CheckpointFailed {
                        task_id,
                        checkpoint_id,
                        error,
                    })) => {
                        tracing::error!(
                            job = %self.job_id,
                            "final checkpoint {} failed on task {}: {}",
                            checkpoint_id,
                            task_id,
                            error
                        );
                        self.abort_checkpoint(final_pending.checkpoint_id, "task failure");
                        final_pending.awaiting.clear();
                        break;
                    }
                    Ok(Some(_)) => continue,
                    Ok(None) | Err(_) => break,
                }
            }
            if !final_pending.reports.is_empty() {
                self.complete_checkpoint(final_pending, true).await?;
            }
            // Give live tasks a bounded moment to finish phase 2 before
            // the cancel token makes them exit.
            let phase2_deadline = tokio::time::Instant::now() + Duration::from_secs(5);
            while tokio::time::Instant::now() < phase2_deadline {
                match tokio::time::timeout(Duration::from_millis(200), self.report_rx.recv()).await
                {
                    Ok(Some(_)) => {}
                    _ => break,
                }
            }
        }
        task_cancel.cancel();
        Ok(())
    }

    /// The newest persisted envelope (for post-run savepoint inspection).
    pub fn last_envelope(&self) -> Option<&CheckpointEnvelope> {
        self.last_envelope.as_ref()
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    fn envelope(job: &str, id: u64, tasks: usize) -> CheckpointEnvelope {
        CheckpointEnvelope {
            job_id: job.to_string(),
            checkpoint_id: id,
            timestamp: now_millis(),
            is_final: false,
            tasks: (0..tasks)
                .map(|i| TaskCheckpointReport {
                    task_id: format!("t{}", i),
                    checkpoint_id: id,
                    pipeline: "p0".to_string(),
                    subtask: i,
                    parallelism: tasks,
                    reader_state: vec![i as u8],
                    writer_state: vec![],
                    commit_infos: vec![],
                })
                .collect(),
        }
    }

    #[test]
    fn store_roundtrip_and_latest() {
        let root = std::env::temp_dir().join(format!("cp-store-test-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&root);
        let store = LocalCheckpointStore::new(&root);
        store.save(&envelope("job-a", 1, 2)).unwrap();
        store.save(&envelope("job-a", 2, 2)).unwrap();
        store.save(&envelope("job-b", 7, 1)).unwrap();

        let latest = store.load_latest("job-a").unwrap().unwrap();
        assert_eq!(latest.checkpoint_id, 2);
        assert_eq!(latest.tasks.len(), 2);
        assert_eq!(
            store.load_latest("job-b").unwrap().unwrap().checkpoint_id,
            7
        );
        assert!(store.load_latest("missing").unwrap().is_none());

        // task state lookup by (pipeline, subtask)
        assert_eq!(latest.task_state("p0", 1).unwrap().reader_state, vec![1]);

        // prune keeps newest N
        store.save(&envelope("job-a", 3, 2)).unwrap();
        store.prune("job-a", 2).unwrap();
        let all = store.load_all("job-a").unwrap();
        assert_eq!(all.len(), 2);
        assert_eq!(all[0].checkpoint_id, 2);

        store.drop_job("job-a").unwrap();
        assert!(store.load_latest("job-a").unwrap().is_none());
        let _ = std::fs::remove_dir_all(&root);
    }

    #[test]
    fn store_saves_unsafe_job_ids() {
        let root = std::env::temp_dir().join(format!("cp-store-safe-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&root);
        let store = LocalCheckpointStore::new(&root);
        store.save(&envelope("../evil/job", 1, 1)).unwrap();
        assert!(store.load_latest("../evil/job").unwrap().is_some());
        let _ = std::fs::remove_dir_all(&root);
    }

    async fn spawn_fake_task(plan: &mut LocalCheckpointPlan, name: &str, delay_ms: u64) {
        let handle = plan.register(TaskRegistration {
            task_id: name.to_string(),
            pipeline: "p0".to_string(),
            subtask: 0,
            parallelism: 1,
        });
        // Simulate a TaskGroup: on trigger, (optionally slowly) report.
        let handle2 = handle.clone();
        let id = name.to_string();
        tokio::spawn(async move {
            loop {
                if let Some(cp_id) = handle2.take_trigger() {
                    if delay_ms > 0 {
                        tokio::time::sleep(Duration::from_millis(delay_ms)).await;
                    }
                    handle2.report(TaskToDriver::Checkpoint(TaskCheckpointReport {
                        task_id: id.clone(),
                        checkpoint_id: cp_id,
                        pipeline: "p0".to_string(),
                        subtask: 0,
                        parallelism: 1,
                        reader_state: vec![],
                        writer_state: vec![],
                        commit_infos: vec![],
                    }));
                    if let Some(CheckpointEvent::Completed(cp)) = handle2.poll_event() {
                        handle2.report(TaskToDriver::CommitDone {
                            task_id: id.clone(),
                            checkpoint_id: cp,
                        });
                    }
                } else if let Some(CheckpointEvent::Completed(cp)) = handle2.poll_event() {
                    handle2.report(TaskToDriver::CommitDone {
                        task_id: id.clone(),
                        checkpoint_id: cp,
                    });
                } else {
                    tokio::time::sleep(Duration::from_millis(5)).await;
                }
            }
        });
    }

    #[tokio::test]
    async fn driver_completes_checkpoint_after_all_reports() {
        let root = std::env::temp_dir().join(format!("cp-driver-test-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&root);
        let mut plan = LocalCheckpointPlan::new(&root, "job-driver", Duration::from_millis(20));
        spawn_fake_task(&mut plan, "fast", 0).await;
        spawn_fake_task(&mut plan, "slow", 30).await;
        let store = LocalCheckpointStore::new(&root);
        let shutdown = tokio_util::sync::CancellationToken::new();
        let task_cancel = Arc::new(tokio_util::sync::CancellationToken::new());
        let driver = plan.build();
        let handle = tokio::spawn(driver.run(shutdown.clone(), task_cancel));

        // Wait until at least one checkpoint hits the store.
        let mut latest = None;
        for _ in 0..100 {
            if let Some(env) = store.load_latest("job-driver").unwrap() {
                latest = Some(env);
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        let env = latest.expect("no checkpoint persisted");
        assert!(env.tasks.len() >= 2, "both tasks must report");

        shutdown.cancel();
        handle.await.unwrap().unwrap();
        let _ = std::fs::remove_dir_all(&root);
    }

    #[tokio::test]
    async fn restore_continues_id_sequence() {
        let root = std::env::temp_dir().join(format!("cp-restore-test-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&root);
        let store = LocalCheckpointStore::new(&root);
        store.save(&envelope("job-r", 5, 1)).unwrap();

        let mut plan = LocalCheckpointPlan::new(&root, "job-r", Duration::from_secs(60))
            .restore_from_latest()
            .unwrap();
        assert_eq!(plan.restore_envelope().unwrap().checkpoint_id, 5);
        spawn_fake_task(&mut plan, "t0", 0).await;
        let driver = plan.build();
        // next id must be 6: trigger one checkpoint manually via run start.
        let shutdown = tokio_util::sync::CancellationToken::new();
        let task_cancel = Arc::new(tokio_util::sync::CancellationToken::new());
        let join = tokio::spawn(driver.run(shutdown.clone(), task_cancel));
        tokio::time::sleep(Duration::from_millis(100)).await;
        shutdown.cancel();
        join.await.unwrap().unwrap();
        let latest = store.load_latest("job-r").unwrap().unwrap();
        // The final checkpoint (or an early interval one) has id >= 6.
        assert!(latest.checkpoint_id >= 6, "got {}", latest.checkpoint_id);
        let _ = std::fs::remove_dir_all(&root);
    }

    #[tokio::test]
    async fn checkpoint_times_out_and_aborts() {
        let root = std::env::temp_dir().join(format!("cp-timeout-test-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&root);
        // A task whose report is slower than the driver timeout.
        let mut plan = LocalCheckpointPlan::new(&root, "job-timeout", Duration::from_millis(20))
            .with_timeout(Duration::from_millis(50));
        spawn_fake_task(&mut plan, "stuck", 10_000).await;
        let store = LocalCheckpointStore::new(&root);
        let shutdown = tokio_util::sync::CancellationToken::new();
        let task_cancel = Arc::new(tokio_util::sync::CancellationToken::new());
        let join = tokio::spawn(plan.build().run(shutdown.clone(), task_cancel.clone()));
        tokio::time::sleep(Duration::from_millis(300)).await;
        shutdown.cancel();
        join.await.unwrap().unwrap();
        // Nothing durable, and the stuck task got cancelled at shutdown.
        assert!(store.load_latest("job-timeout").unwrap().is_none());
        assert!(task_cancel.is_cancelled());
        let _ = std::fs::remove_dir_all(&root);
    }
}
