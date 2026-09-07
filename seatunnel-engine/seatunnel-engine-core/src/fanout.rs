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

//! Fan-out sink multiplexer: one pipeline reader broadcasting rows to N
//! sink writers **concurrently** (low latency — a slow sink never blocks
//! the others or the reader until its bounded buffer fills).
//!
//! Each inner writer runs on its own tokio task fed by a bounded channel:
//!
//! ```text
//! reader ──► FanoutSinkWriter ──┬── channel A ──► writer task A (Kafka)
//!                              ├── channel B ──► writer task B (JDBC)
//!                              └── channel C ──► writer task C (Redis)
//! ```
//!
//! `write()` only enqueues (cheap); the actual sink I/O happens on the
//! writer tasks. `prepare_commit()` broadcasts a flush command and awaits
//! every ack, preserving the engine's "sinks flushed before the reader
//! snapshot" checkpoint order.
//!
//! Failure policy (Java has no direct equivalent; this mirrors the common
//! `on-sink-failure` knob):
//! - [`SinkFailurePolicy::Fail`] (default): a dead writer fails the whole
//!   task on the next interaction — strict, at-least-once consistent.
//! - [`SinkFailurePolicy::Isolate`]: the dead writer is removed and the
//!   remaining sinks continue; a restart replays the missing rows from
//!   the reader checkpoint (best-effort continuity).

use std::collections::HashMap;
use std::panic::AssertUnwindSafe;
use std::sync::Arc;
use std::time::Duration;

use futures::FutureExt;
use seatunnel_api::row::Row;
use seatunnel_api::schema::SchemaChangeEvent;
use seatunnel_api::sink::sink_committer::SinkCommitter;
use seatunnel_api::sink::sink_writer::SinkWriter;
use tokio::sync::{Mutex, mpsc, oneshot};
use tracing::error;

use crate::connector_factory::{BoxedSinkCommitter, BoxedSinkWriter};

/// Bounded per-sink buffer; a slow sink backpressures the reader only
/// after this many queued commands.
pub const FANOUT_CHANNEL_CAPACITY: usize = 1024;

/// Default ceiling for every bounded wait on an inner sink (channel
/// sends and acks). Without it one hung sink freezes the whole task
/// loop — cancel included — while the heartbeat keeps claiming RUNNING.
pub const DEFAULT_SINK_ACK_TIMEOUT: Duration = Duration::from_secs(60);

/// Behavior when an inner sink writer dies.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum SinkFailurePolicy {
    /// Fail the whole task (default; strict at-least-once consistency).
    #[default]
    Fail,
    /// Remove the failed sink and continue with the rest.
    Isolate,
}

impl SinkFailurePolicy {
    pub fn parse(s: &str) -> Self {
        match s.trim().to_lowercase().as_str() {
            "isolate" | "skip" | "continue" => SinkFailurePolicy::Isolate,
            _ => SinkFailurePolicy::Fail,
        }
    }
}

/// Commands flowing from the multiplexer to a writer task. Channel order
/// is preserved per sink, so a schema change acked before a write means
/// the writer applied it before that row.
enum SinkCommand {
    Open(oneshot::Sender<anyhow::Result<()>>),
    Write(Row),
    SchemaChange(Box<SchemaChangeEvent>, oneshot::Sender<anyhow::Result<()>>),
    PrepareCommit(u64, oneshot::Sender<anyhow::Result<Vec<Vec<u8>>>>),
    SnapshotState(oneshot::Sender<anyhow::Result<Vec<u8>>>),
    Close(oneshot::Sender<anyhow::Result<()>>),
}

/// Shared sink-health registry: sink name → last error (empty value =
/// healthy). Written by writer tasks, read by the multiplexer and
/// diagnostics.
type SinkErrors = Arc<Mutex<HashMap<String, String>>>;

/// How long a fan-out worker waits for a command before nudging the inner
/// writer's idle flush (`SinkWriter::poll_flush`).
const FANOUT_IDLE_TICK: std::time::Duration = std::time::Duration::from_millis(100);

async fn run_sink_worker(
    name: String,
    writer: BoxedSinkWriter,
    rx: mpsc::Receiver<SinkCommand>,
    errors: SinkErrors,
) {
    // Panic isolation: an inner writer's panic must surface as a
    // diagnosable dead sink (reason recorded in `errors`), not vanish
    // into an aborted task whose JoinError nobody inspects.
    let loop_name = name.clone();
    let loop_errors = Arc::clone(&errors);
    let inner = async move { run_sink_worker_loop(loop_name, writer, rx, loop_errors).await };
    match AssertUnwindSafe(inner).catch_unwind().await {
        Ok(()) => {}
        Err(payload) => {
            let msg = panic_message(&payload);
            error!(
                "fan-out sink '{}' writer PANICKED — the sink is dead: {}",
                name, msg
            );
            errors
                .lock()
                .await
                .insert(name, format!("writer task panicked: {msg}"));
        }
    }
}

/// Lower-cased panic payload for diagnostics.
fn panic_message(payload: &Box<dyn std::any::Any + Send>) -> String {
    if let Some(s) = payload.downcast_ref::<&str>() {
        (*s).to_string()
    } else if let Some(s) = payload.downcast_ref::<String>() {
        s.clone()
    } else {
        "unknown panic".to_string()
    }
}

async fn run_sink_worker_loop(
    name: String,
    mut writer: BoxedSinkWriter,
    mut rx: mpsc::Receiver<SinkCommand>,
    errors: SinkErrors,
) {
    loop {
        let command = match tokio::time::timeout(FANOUT_IDLE_TICK, rx.recv()).await {
            Ok(Some(command)) => command,
            Ok(None) => break, // multiplexer dropped the channel
            Err(_) => {
                // Idle tick: flush tail records whose linger has elapsed so
                // they do not wait for the next write or checkpoint.
                if let Err(e) = writer.poll_flush().await {
                    error!(
                        "fan-out sink '{}' idle flush failed: {e:#} — the sink is dead",
                        name
                    );
                    errors.lock().await.insert(name.clone(), format!("{e:#}"));
                    break;
                }
                continue;
            }
        };
        let result: anyhow::Result<()> = match command {
            SinkCommand::Open(ack) => {
                let _ = ack.send(writer.open().await);
                continue;
            }
            SinkCommand::Write(row) => writer.write(row).await.map(|_| ()),
            SinkCommand::SchemaChange(event, ack) => {
                let _ = ack.send(writer.apply_schema_change(&event).await);
                continue;
            }
            SinkCommand::PrepareCommit(checkpoint_id, ack) => {
                let _ = ack.send(writer.prepare_commit(checkpoint_id).await);
                continue;
            }
            SinkCommand::SnapshotState(ack) => {
                let _ = ack.send(writer.snapshot_state().await);
                continue;
            }
            SinkCommand::Close(ack) => {
                let _ = ack.send(writer.close().await);
                break;
            }
        };
        if let Err(e) = result {
            error!(
                "fan-out sink '{}' writer failed — the sink is dead: {e:#}",
                name
            );
            errors.lock().await.insert(name.clone(), format!("{e:#}"));
            // Exiting drops `rx`; the multiplexer detects the closed
            // channel on its next interaction.
            break;
        }
    }
}

struct SinkHandle {
    name: String,
    /// The inner writer, held until `open` spawns its task.
    spawn: Option<BoxedSinkWriter>,
    tx: Option<mpsc::Sender<SinkCommand>>,
    join: Option<tokio::task::JoinHandle<()>>,
    dead: bool,
}

/// A [`SinkWriter`] multiplexing rows to N inner writers, each on its own
/// task with a bounded queue — drop-in wherever a `BoxedSinkWriter` fits
/// (TaskGroup needs no changes).
pub struct FanoutSinkWriter {
    policy: SinkFailurePolicy,
    handles: Vec<SinkHandle>,
    errors: SinkErrors,
    /// Ceiling for channel sends and ack waits to any inner sink. A hung
    /// sink fails (policy=fail) or is isolated (policy=isolate) at this
    /// deadline instead of freezing the task loop forever.
    ack_timeout: Duration,
}

impl FanoutSinkWriter {
    /// `writers` pairs a diagnostic name with each inner writer; spawned
    /// lazily on [`SinkWriter::open`].
    pub fn new(writers: Vec<(String, BoxedSinkWriter)>, policy: SinkFailurePolicy) -> Self {
        Self::with_ack_timeout(writers, policy, DEFAULT_SINK_ACK_TIMEOUT)
    }

    /// Like [`Self::new`], with an explicit ceiling for channel sends and
    /// ack waits (`pipeline.sink-ack-timeout-ms`).
    pub fn with_ack_timeout(
        writers: Vec<(String, BoxedSinkWriter)>,
        policy: SinkFailurePolicy,
        ack_timeout: Duration,
    ) -> Self {
        FanoutSinkWriter {
            policy,
            handles: writers
                .into_iter()
                .map(|(name, writer)| SinkHandle {
                    name,
                    spawn: Some(writer),
                    tx: None,
                    join: None,
                    dead: false,
                })
                .collect(),
            errors: Arc::new(Mutex::new(HashMap::new())),
            ack_timeout,
        }
    }

    fn alive(&self) -> Vec<usize> {
        (0..self.handles.len())
            .filter(|i| !self.handles[*i].dead)
            .collect()
    }

    /// Handle a closed channel (writer task died). Returns Err under the
    /// Fail policy, marks the sink dead under Isolate.
    fn on_channel_closed(&mut self, idx: usize, op: &str) -> anyhow::Result<()> {
        let reason = self
            .errors
            .try_lock()
            .ok()
            .and_then(|errors| errors.get(&self.handles[idx].name).cloned())
            .unwrap_or_else(|| "writer task terminated".to_string());
        self.mark_dead(idx, format!("{reason} (op: {op})"))
    }

    /// Mark a sink dead. Fail policy → task error; Isolate → ERROR log
    /// and continue with the remaining sinks.
    fn mark_dead(&mut self, idx: usize, reason: String) -> anyhow::Result<()> {
        if self.handles[idx].dead {
            return Ok(());
        }
        self.handles[idx].dead = true;
        let name = self.handles[idx].name.clone();
        match self.policy {
            SinkFailurePolicy::Fail => Err(anyhow::anyhow!(
                "fan-out sink '{}' failed (on-sink-failure=fail): {}",
                name,
                reason
            )),
            SinkFailurePolicy::Isolate => {
                error!(
                    "fan-out sink '{}' isolated (on-sink-failure=isolate): {}",
                    name, reason
                );
                Ok(())
            }
        }
    }

    /// Send a command to one inner sink with a bounded wait. A full
    /// channel (hung inner writer) times out exactly like a lost ack —
    /// it can never freeze the task loop again.
    async fn send_command(
        &mut self,
        idx: usize,
        command: SinkCommand,
        op: &str,
    ) -> anyhow::Result<()> {
        let tx = self.handles[idx].tx.as_ref().expect("opened").clone();
        match tokio::time::timeout(self.ack_timeout, tx.send(command)).await {
            Ok(Ok(())) => Ok(()),
            Ok(Err(_)) => self.on_channel_closed(idx, op),
            Err(_) => self.mark_dead(
                idx,
                format!(
                    "channel send timed out after {:?} — the sink writer is hung",
                    self.ack_timeout
                ),
            ),
        }
    }

    /// Broadcast a command carrying an ack channel and await every ack
    /// (concurrently, each bounded by the ack timeout). Used by
    /// open/prepare_commit/schema-change/snapshot/close.
    async fn broadcast_and_await<T, F>(
        &mut self,
        op: &'static str,
        make_command: F,
    ) -> anyhow::Result<Vec<T>>
    where
        F: Fn() -> (SinkCommand, oneshot::Receiver<anyhow::Result<T>>),
    {
        let mut acks = Vec::new();
        for idx in self.alive() {
            let (command, ack) = make_command();
            self.send_command(idx, command, op).await?;
            acks.push((idx, ack));
        }
        let mut results = Vec::with_capacity(acks.len());
        for (idx, ack) in acks {
            match tokio::time::timeout(self.ack_timeout, ack).await {
                Ok(Ok(Ok(value))) => results.push(value),
                Ok(Ok(Err(e))) => {
                    let reason = format!("{e:#} (op: {op})");
                    self.errors
                        .lock()
                        .await
                        .insert(self.handles[idx].name.clone(), reason.clone());
                    self.mark_dead(idx, reason)?;
                }
                Ok(Err(_)) => self.on_channel_closed(idx, op)?,
                Err(_) => {
                    let name = self.handles[idx].name.clone();
                    error!(
                        "fan-out sink '{}' ack timed out after {:?} (op: {}) — the sink \
                         writer is hung",
                        name, self.ack_timeout, op
                    );
                    self.mark_dead(
                        idx,
                        format!(
                            "ack timed out after {:?} — the sink writer is hung (op: {})",
                            self.ack_timeout, op
                        ),
                    )?;
                }
            }
        }
        Ok(results)
    }
}

impl SinkWriter for FanoutSinkWriter {
    type Input = Row;
    type WriterState = Vec<u8>;
    type CommitInfo = Vec<u8>;

    fn open(&mut self) -> seatunnel_api::sink::sink_writer::WriterFuture<'_, ()> {
        Box::pin(async move {
            // Spawn a task per writer, then run the open handshake.
            for idx in 0..self.handles.len() {
                let handle = &mut self.handles[idx];
                if handle.tx.is_some() {
                    continue; // already opened
                }
                let writer = handle
                    .spawn
                    .take()
                    .expect("writer present before first open");
                let (tx, rx) = mpsc::channel(FANOUT_CHANNEL_CAPACITY);
                let name = handle.name.clone();
                let errors = Arc::clone(&self.errors);
                handle.join = Some(tokio::spawn(run_sink_worker(name, writer, rx, errors)));
                handle.tx = Some(tx);
            }
            self.broadcast_and_await("open", || {
                let (ack_tx, ack_rx) = oneshot::channel();
                (SinkCommand::Open(ack_tx), ack_rx)
            })
            .await
            .map(|_| ())
        })
    }

    fn write(
        &mut self,
        record: Self::Input,
    ) -> seatunnel_api::sink::sink_writer::WriterFuture<'_, ()> {
        Box::pin(async move {
            // The last live sink receives the record by move; the rest clone.
            let alive = self.alive();
            if alive.is_empty() {
                // Isolate policy with every sink gone must never become a
                // silent black hole: fail the task; the checkpoint replay
                // keeps delivery at-least-once.
                error!(
                    "fan-out: every sink is dead under on-sink-failure=isolate — failing the \
                     task instead of silently dropping rows"
                );
                anyhow::bail!(
                    "all fan-out sinks are isolated/dead (on-sink-failure=isolate); rows \
                     can no longer be delivered"
                );
            }
            let last = alive[alive.len() - 1];
            for &idx in &alive[..alive.len() - 1] {
                // Fail policy propagates (task error); Isolate returns
                // Ok after logging, so remaining sinks keep receiving.
                self.send_command(idx, SinkCommand::Write(record.clone()), "write")
                    .await?;
            }
            self.send_command(last, SinkCommand::Write(record), "write")
                .await?;
            // Under Isolate a dead LAST sink returns Ok — but that row
            // was moved into it and delivered NOWHERE. A total delivery
            // failure must stay loud under both policies.
            if self.handles[last].dead {
                let name = self.handles[last].name.clone();
                anyhow::bail!(
                    "fan-out sink '{}' died mid-write under on-sink-failure=isolate and no \
                     other sink received the row — failing the task",
                    name
                );
            }
            Ok(())
        })
    }

    fn prepare_commit(
        &mut self,
        checkpoint_id: u64,
    ) -> seatunnel_api::sink::sink_writer::WriterFuture<'_, Vec<Self::CommitInfo>> {
        Box::pin(async move {
            let cp_id = checkpoint_id;
            let commits = self
                .broadcast_and_await("prepare_commit", move || {
                    let (ack_tx, ack_rx) = oneshot::channel();
                    (SinkCommand::PrepareCommit(cp_id, ack_tx), ack_rx)
                })
                .await?;
            // Encode per-sink commit-info groups so the fan-out committer
            // can route phase 2 to each sink's own committer (a flat list
            // would lose the sink boundaries).
            let entries: Vec<FanoutCommitEntry> = self
                .alive()
                .into_iter()
                .enumerate()
                .map(|(i, idx)| FanoutCommitEntry {
                    sink: self.handles[idx].name.clone(),
                    infos: commits.get(i).cloned().unwrap_or_default(),
                })
                .collect();
            let encoded = serde_json::to_vec(&FanoutCommitInfos { entries })?;
            Ok(vec![encoded])
        })
    }

    fn snapshot_state(&mut self) -> seatunnel_api::sink::sink_writer::WriterFuture<'_, Vec<u8>> {
        Box::pin(async move {
            let states = self
                .broadcast_and_await("snapshot_state", || {
                    let (ack_tx, ack_rx) = oneshot::channel();
                    (SinkCommand::SnapshotState(ack_tx), ack_rx)
                })
                .await?;
            let merged: HashMap<String, Vec<u8>> = self
                .alive()
                .into_iter()
                .enumerate()
                .map(|(i, idx)| {
                    (
                        self.handles[idx].name.clone(),
                        states.get(i).cloned().unwrap_or_default(),
                    )
                })
                .collect();
            Ok(serde_json::to_vec(&merged)?)
        })
    }

    fn close(&mut self) -> seatunnel_api::sink::sink_writer::WriterFuture<'_, ()> {
        Box::pin(async move {
            if let Err(e) = self
                .broadcast_and_await("close", || {
                    let (ack_tx, ack_rx) = oneshot::channel();
                    (SinkCommand::Close(ack_tx), ack_rx)
                })
                .await
            {
                error!("fan-out close reported failures: {e:#}");
            }
            for handle in &mut self.handles {
                handle.tx = None;
                if let Some(join) = handle.join.take() {
                    match join.await {
                        Ok(()) => {}
                        Err(e) if e.is_cancelled() => {
                            error!(
                                "fan-out sink '{}' writer task was cancelled during close",
                                handle.name
                            );
                        }
                        Err(e) => {
                            error!(
                                "fan-out sink '{}' writer task PANICKED during close: {}",
                                handle.name, e
                            );
                        }
                    }
                }
            }
            Ok(())
        })
    }

    fn apply_schema_change(
        &mut self,
        event: &SchemaChangeEvent,
    ) -> seatunnel_api::sink::sink_writer::WriterFuture<'_, ()> {
        let event = event.clone();
        Box::pin(async move {
            // Ack awaited per sink: rows enqueued after this call are
            // written with the new shape everywhere.
            self.broadcast_and_await("apply_schema_change", move || {
                let (ack_tx, ack_rx) = oneshot::channel();
                (
                    SinkCommand::SchemaChange(Box::new(event.clone()), ack_tx),
                    ack_rx,
                )
            })
            .await
            .map(|_| ())
        })
    }
}

/// Per-sink commit-info group produced by [`FanoutSinkWriter::prepare_commit`].
#[derive(Debug, serde::Serialize, serde::Deserialize)]
pub(crate) struct FanoutCommitInfos {
    pub entries: Vec<FanoutCommitEntry>,
}

#[derive(Debug, serde::Serialize, serde::Deserialize)]
pub(crate) struct FanoutCommitEntry {
    pub sink: String,
    pub infos: Vec<Vec<u8>>,
}

/// Fan-out 2PC committer: parses the structured commit info produced by
/// [`FanoutSinkWriter::prepare_commit`] and routes each sink's phase-2
/// commit/abort to its own committer. Sinks without committers are skipped.
pub struct FanoutCommitter {
    committers: Vec<(String, Option<BoxedSinkCommitter>)>,
}

impl FanoutCommitter {
    /// `None` when no inner sink has a committer (nothing to do at phase 2).
    pub fn new(committers: Vec<(String, Option<BoxedSinkCommitter>)>) -> Option<Self> {
        if committers.iter().any(|(_, c)| c.is_some()) {
            Some(FanoutCommitter { committers })
        } else {
            None
        }
    }

    fn split(commit_infos: Vec<Vec<u8>>) -> anyhow::Result<HashMap<String, Vec<Vec<u8>>>> {
        let Some(first) = commit_infos.into_iter().next() else {
            return Ok(HashMap::new());
        };
        let parsed: FanoutCommitInfos = serde_json::from_slice(&first)
            .map_err(|e| anyhow::anyhow!("fan-out commit info decode: {}", e))?;
        Ok(parsed
            .entries
            .into_iter()
            .map(|e| (e.sink, e.infos))
            .collect())
    }
}

impl SinkCommitter for FanoutCommitter {
    type CommitInfo = Vec<u8>;
    type AggregatedCommitInfo = serde_json::Value;

    fn commit(
        &mut self,
        commit_infos: Vec<Self::CommitInfo>,
    ) -> seatunnel_api::sink::sink_committer::CommitterFuture<'_, Self::AggregatedCommitInfo> {
        Box::pin(async move {
            let groups = Self::split(commit_infos)?;
            let mut aggregated = serde_json::Map::new();
            for (name, committer) in &mut self.committers {
                if let (Some(committer), Some(infos)) = (committer, groups.get(name)) {
                    aggregated.insert(name.clone(), committer.commit(infos.clone()).await?);
                }
            }
            Ok(serde_json::Value::Object(aggregated))
        })
    }

    fn abort(
        &mut self,
        commit_infos: Vec<Self::CommitInfo>,
    ) -> seatunnel_api::sink::sink_committer::CommitterFuture<'_, ()> {
        Box::pin(async move {
            let groups = Self::split(commit_infos)?;
            for (name, committer) in &mut self.committers {
                if let (Some(committer), Some(infos)) = (committer, groups.get(name)) {
                    committer.abort(infos.clone()).await?;
                }
            }
            Ok(())
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use seatunnel_api::row::{Field, RowKind};
    use std::sync::{Arc, Mutex};
    use std::time::{Duration, Instant};

    /// Instrumented sink writer: logs every operation, optionally delays
    /// writes, hangs on prepare_commit, panics, or fails on the Nth write.
    struct TestWriter {
        name: String,
        ops: Arc<Mutex<Vec<String>>>,
        write_delay: Duration,
        fail_on_write: Option<usize>,
        hang_prepare: Option<Duration>,
        panic_on_write: bool,
        writes: usize,
    }

    impl TestWriter {
        fn new(name: &str, ops: Arc<Mutex<Vec<String>>>) -> Self {
            TestWriter {
                name: name.to_string(),
                ops,
                write_delay: Duration::ZERO,
                fail_on_write: None,
                hang_prepare: None,
                panic_on_write: false,
                writes: 0,
            }
        }

        fn with_delay(mut self, delay: Duration) -> Self {
            self.write_delay = delay;
            self
        }

        fn failing_on(mut self, nth: usize) -> Self {
            self.fail_on_write = Some(nth);
            self
        }

        fn hanging_prepare(mut self, delay: Duration) -> Self {
            self.hang_prepare = Some(delay);
            self
        }

        fn panicking_on_write(mut self) -> Self {
            self.panic_on_write = true;
            self
        }

        fn log(&self, op: &str) {
            self.ops
                .lock()
                .unwrap()
                .push(format!("{}:{}", self.name, op));
        }
    }

    impl SinkWriter for TestWriter {
        type Input = Row;
        type WriterState = Vec<u8>;
        type CommitInfo = String;

        fn open(&mut self) -> seatunnel_api::sink::sink_writer::WriterFuture<'_, ()> {
            self.log("open");
            Box::pin(async { Ok(()) })
        }

        fn write(
            &mut self,
            _record: Self::Input,
        ) -> seatunnel_api::sink::sink_writer::WriterFuture<'_, ()> {
            self.writes += 1;
            let fail = self.fail_on_write == Some(self.writes);
            let delay = self.write_delay;
            let panic_on_write = self.panic_on_write;
            let entry = format!("{}:write{}", self.name, self.writes);
            let ops = Arc::clone(&self.ops);
            Box::pin(async move {
                if delay > Duration::ZERO {
                    tokio::time::sleep(delay).await;
                }
                ops.lock().unwrap().push(entry);
                if fail {
                    anyhow::bail!("injected failure");
                }
                if panic_on_write {
                    panic!("injected panic");
                }
                Ok(())
            })
        }

        fn prepare_commit(
            &mut self,
            _checkpoint_id: u64,
        ) -> seatunnel_api::sink::sink_writer::WriterFuture<'_, Vec<Self::CommitInfo>> {
            self.log("flush");
            let commit = format!("{}-commit", self.name);
            let hang = self.hang_prepare;
            Box::pin(async move {
                if let Some(delay) = hang {
                    tokio::time::sleep(delay).await;
                }
                Ok(vec![commit])
            })
        }

        fn snapshot_state(
            &mut self,
        ) -> seatunnel_api::sink::sink_writer::WriterFuture<'_, Vec<u8>> {
            let state = format!("{}-state", self.name).into_bytes();
            Box::pin(async move { Ok(state) })
        }

        fn close(&mut self) -> seatunnel_api::sink::sink_writer::WriterFuture<'_, ()> {
            self.log("close");
            Box::pin(async { Ok(()) })
        }
    }

    fn row(id: i64) -> Row {
        let mut r = Row::new(RowKind::Insert, 1);
        r.set(0, Field::Int64(id));
        r
    }

    fn boxed(w: TestWriter) -> BoxedSinkWriter {
        Box::new(crate::connector_factory::SinkWriterAdapter { inner: w })
    }

    #[tokio::test]
    async fn fanout_writes_flush_and_close_all_sinks_in_order() {
        let ops = Arc::new(Mutex::new(Vec::new()));
        let mut mux = FanoutSinkWriter::new(
            vec![
                (
                    "a".to_string(),
                    boxed(TestWriter::new("a", Arc::clone(&ops))),
                ),
                (
                    "b".to_string(),
                    boxed(TestWriter::new("b", Arc::clone(&ops))),
                ),
            ],
            SinkFailurePolicy::Fail,
        );
        mux.open().await.unwrap();
        mux.write(row(1)).await.unwrap();
        mux.write(row(2)).await.unwrap();
        mux.prepare_commit(1).await.unwrap();
        mux.close().await.unwrap();

        let ops = ops.lock().unwrap().clone();
        // Per-sink ordering: writes precede the flush.
        let a = ops
            .iter()
            .filter(|o| o.starts_with("a:"))
            .cloned()
            .collect::<Vec<_>>();
        let b = ops
            .iter()
            .filter(|o| o.starts_with("b:"))
            .cloned()
            .collect::<Vec<_>>();
        assert_eq!(
            a,
            vec!["a:open", "a:write1", "a:write2", "a:flush", "a:close"]
        );
        assert_eq!(
            b,
            vec!["b:open", "b:write1", "b:write2", "b:flush", "b:close"]
        );
    }

    #[tokio::test]
    async fn slow_sink_does_not_block_writes_to_the_mux() {
        let ops = Arc::new(Mutex::new(Vec::new()));
        let mut mux = FanoutSinkWriter::new(
            vec![
                (
                    "slow".to_string(),
                    boxed(
                        TestWriter::new("slow", Arc::clone(&ops))
                            .with_delay(Duration::from_millis(300)),
                    ),
                ),
                (
                    "fast".to_string(),
                    boxed(TestWriter::new("fast", Arc::clone(&ops))),
                ),
            ],
            SinkFailurePolicy::Fail,
        );
        mux.open().await.unwrap();

        // The 300ms slow sink must not stall the write path — enqueueing
        // only.
        let start = Instant::now();
        mux.write(row(1)).await.unwrap();
        assert!(
            start.elapsed() < Duration::from_millis(150),
            "write blocked for {:?}",
            start.elapsed()
        );

        // The fast sink processes the row well before the slow one.
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert!(ops.lock().unwrap().contains(&"fast:write1".to_string()));
        assert!(!ops.lock().unwrap().contains(&"slow:write1".to_string()));

        // prepare_commit waits for BOTH (flush ordering guarantee).
        mux.prepare_commit(1).await.unwrap();
        assert!(ops.lock().unwrap().contains(&"slow:write1".to_string()));
        assert!(ops.lock().unwrap().contains(&"slow:flush".to_string()));
        mux.close().await.unwrap();
    }

    #[tokio::test]
    async fn fail_policy_propagates_writer_death() {
        let ops = Arc::new(Mutex::new(Vec::new()));
        let mut mux = FanoutSinkWriter::new(
            vec![(
                "doomed".to_string(),
                boxed(TestWriter::new("doomed", Arc::clone(&ops)).failing_on(1)),
            )],
            SinkFailurePolicy::Fail,
        );
        mux.open().await.unwrap();
        mux.write(row(1)).await.unwrap(); // enqueued; the writer fails
        tokio::time::sleep(Duration::from_millis(50)).await; // let it die
        let err = mux.write(row(2)).await.unwrap_err();
        assert!(err.to_string().contains("doomed"), "{}", err);
    }

    #[tokio::test]
    async fn isolate_policy_removes_dead_sink_and_continues() {
        let ops = Arc::new(Mutex::new(Vec::new()));
        let mut mux = FanoutSinkWriter::new(
            vec![
                (
                    "doomed".to_string(),
                    boxed(TestWriter::new("doomed", Arc::clone(&ops)).failing_on(1)),
                ),
                (
                    "healthy".to_string(),
                    boxed(TestWriter::new("healthy", Arc::clone(&ops))),
                ),
            ],
            SinkFailurePolicy::Isolate,
        );
        mux.open().await.unwrap();
        mux.write(row(1)).await.unwrap();
        tokio::time::sleep(Duration::from_millis(50)).await; // doomed dies
        mux.write(row(2)).await.unwrap(); // skipped for doomed, ok for healthy
        mux.prepare_commit(1).await.unwrap(); // doomed skipped, healthy flushes
        mux.close().await.unwrap();

        let ops = ops.lock().unwrap().clone();
        assert!(ops.contains(&"healthy:write1".to_string()));
        assert!(ops.contains(&"healthy:write2".to_string()));
        assert!(ops.contains(&"healthy:flush".to_string()));
        // doomed wrote its failing row only
        let doomed_writes = ops.iter().filter(|o| o.starts_with("doomed:write")).count();
        assert_eq!(doomed_writes, 1);
    }

    #[tokio::test]
    async fn schema_change_forwarded_and_awaited_everywhere() {
        let ops = Arc::new(Mutex::new(Vec::new()));
        struct SchemaWriter {
            name: String,
            ops: Arc<Mutex<Vec<String>>>,
        }
        impl SinkWriter for SchemaWriter {
            type Input = Row;
            type WriterState = Vec<u8>;
            type CommitInfo = String;
            fn open(&mut self) -> seatunnel_api::sink::sink_writer::WriterFuture<'_, ()> {
                Box::pin(async { Ok(()) })
            }
            fn write(
                &mut self,
                _record: Self::Input,
            ) -> seatunnel_api::sink::sink_writer::WriterFuture<'_, ()> {
                self.ops
                    .lock()
                    .unwrap()
                    .push(format!("{}:write", self.name));
                Box::pin(async { Ok(()) })
            }
            fn prepare_commit(
                &mut self,
                _checkpoint_id: u64,
            ) -> seatunnel_api::sink::sink_writer::WriterFuture<'_, Vec<Self::CommitInfo>>
            {
                Box::pin(async { Ok(vec![]) })
            }
            fn snapshot_state(
                &mut self,
            ) -> seatunnel_api::sink::sink_writer::WriterFuture<'_, Vec<u8>> {
                Box::pin(async { Ok(Vec::new()) })
            }
            fn close(&mut self) -> seatunnel_api::sink::sink_writer::WriterFuture<'_, ()> {
                Box::pin(async { Ok(()) })
            }
            fn apply_schema_change(
                &mut self,
                _event: &SchemaChangeEvent,
            ) -> seatunnel_api::sink::sink_writer::WriterFuture<'_, ()> {
                self.ops.lock().unwrap().push(format!("{}:ddl", self.name));
                Box::pin(async { Ok(()) })
            }
        }

        let mut mux = FanoutSinkWriter::new(
            vec![
                (
                    "a".to_string(),
                    Box::new(crate::connector_factory::SinkWriterAdapter {
                        inner: SchemaWriter {
                            name: "a".into(),
                            ops: Arc::clone(&ops),
                        },
                    }),
                ),
                (
                    "b".to_string(),
                    Box::new(crate::connector_factory::SinkWriterAdapter {
                        inner: SchemaWriter {
                            name: "b".into(),
                            ops: Arc::clone(&ops),
                        },
                    }),
                ),
            ],
            SinkFailurePolicy::Fail,
        );
        mux.open().await.unwrap();
        let event =
            SchemaChangeEvent::new("db.t", vec![seatunnel_api::SchemaChange::drop_column("x")]);
        mux.apply_schema_change(&event).await.unwrap();
        let ops_now = ops.lock().unwrap().clone();
        assert!(ops_now.contains(&"a:ddl".to_string()));
        assert!(ops_now.contains(&"b:ddl".to_string()));
        mux.close().await.unwrap();
    }

    #[tokio::test]
    async fn fail_policy_fails_prepare_commit_on_hung_sink_ack() {
        let ops = Arc::new(Mutex::new(Vec::new()));
        let mut mux = FanoutSinkWriter::with_ack_timeout(
            vec![(
                "hung".to_string(),
                boxed(
                    TestWriter::new("hung", Arc::clone(&ops))
                        .hanging_prepare(Duration::from_secs(5)),
                ),
            )],
            SinkFailurePolicy::Fail,
            Duration::from_millis(200),
        );
        mux.open().await.unwrap();
        let started = Instant::now();
        let err = mux.prepare_commit(1).await.unwrap_err();
        let elapsed = started.elapsed();
        assert!(
            err.to_string().contains("timed out") && err.to_string().contains("hung"),
            "error must name the hung sink and the timeout: {err}"
        );
        assert!(
            elapsed < Duration::from_secs(2),
            "ack wait must be bounded, took {elapsed:?}"
        );
    }

    #[tokio::test]
    async fn isolate_policy_marks_hung_sink_dead_and_continues() {
        let ops = Arc::new(Mutex::new(Vec::new()));
        let mut mux = FanoutSinkWriter::with_ack_timeout(
            vec![
                (
                    "hung".to_string(),
                    boxed(
                        TestWriter::new("hung", Arc::clone(&ops))
                            .hanging_prepare(Duration::from_secs(5)),
                    ),
                ),
                (
                    "healthy".to_string(),
                    boxed(TestWriter::new("healthy", Arc::clone(&ops))),
                ),
            ],
            SinkFailurePolicy::Isolate,
            Duration::from_millis(200),
        );
        mux.open().await.unwrap();
        mux.write(row(1)).await.unwrap();
        // The hung sink's ack times out, it is isolated, and the
        // checkpoint still completes against the healthy sink.
        mux.prepare_commit(1).await.unwrap();
        mux.close().await.unwrap();
        assert!(ops.lock().unwrap().contains(&"healthy:flush".to_string()));
    }

    #[tokio::test]
    async fn isolate_with_every_sink_dead_fails_writes_loudly() {
        let ops = Arc::new(Mutex::new(Vec::new()));
        let mut mux = FanoutSinkWriter::with_ack_timeout(
            vec![(
                "doomed".to_string(),
                boxed(TestWriter::new("doomed", Arc::clone(&ops)).failing_on(1)),
            )],
            SinkFailurePolicy::Isolate,
            Duration::from_secs(5),
        );
        mux.open().await.unwrap();
        mux.write(row(1)).await.unwrap(); // enqueued; the writer dies
        tokio::time::sleep(Duration::from_millis(100)).await;
        // Row 2 was moved into the dying last sink: loud failure with the
        // mid-write message, not a silent drop.
        let err = mux.write(row(2)).await.unwrap_err();
        assert!(
            err.to_string().contains("died mid-write"),
            "last-sink death must fail loudly, got: {err}"
        );
        // Row 3 hits the already-all-dead mux: loud failure too.
        let err = mux.write(row(3)).await.unwrap_err();
        assert!(
            err.to_string().contains("all fan-out sinks"),
            "all-dead isolate must fail loudly, got: {err}"
        );
    }

    #[tokio::test]
    async fn panicking_inner_writer_is_surfaced_with_reason() {
        let ops = Arc::new(Mutex::new(Vec::new()));
        let mut mux = FanoutSinkWriter::new(
            vec![(
                "panicky".to_string(),
                boxed(TestWriter::new("panicky", Arc::clone(&ops)).panicking_on_write()),
            )],
            SinkFailurePolicy::Fail,
        );
        mux.open().await.unwrap();
        mux.write(row(1)).await.unwrap(); // enqueued; the writer panics
        tokio::time::sleep(Duration::from_millis(150)).await;
        let err = mux.write(row(2)).await.unwrap_err();
        assert!(
            err.to_string().contains("panicked"),
            "panic must surface in the error chain, got: {err}"
        );
    }
}
