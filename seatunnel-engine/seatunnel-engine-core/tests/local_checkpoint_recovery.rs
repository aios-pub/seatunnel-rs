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

//! Engine-level fault-injection verification of the local checkpoint
//! protocol (the in-process analog of Java Zeta's checkpoint tests).
//!
//! A modeled exactly-once sink (records become visible ONLY at 2PC phase 2
//! or at restore-settlement, mirroring MySQL XA semantics — idempotent by
//! a watermark that plays the role of the xid) is driven through the REAL
//! `LocalCheckpointDriver` + `TaskGroup` machinery. Sessions are run back
//! to back against the same state directory; every session except the
//! last is killed mid-flight (JoinHandle::abort ≈ kill -9: no final
//! checkpoint, no close). The final session shuts down gracefully.
//!
//! After the last session the committed output MUST be exactly
//! `0..=watermark` — no duplicates, no gaps — across the whole crash
//! matrix:
//!   A) crash right after sink phase-1 (before the envelope is persisted)
//!   B) crash right after a new envelope appears on disk (persisted, phase
//!      2 possibly not yet run)
//!   C) crash mid-stream with no checkpoint in flight
//!   D) graceful shutdown (final checkpoint + tail commit)

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use seatunnel_api::row::{Field, Row};
use seatunnel_api::sink::sink_committer::{CommitterFuture, SinkCommitter};
use seatunnel_api::sink::sink_writer::SinkWriter;
use seatunnel_api::source::source_reader::PollResult;
use seatunnel_api::source::source_reader::SourceReader;
use serde::{Deserialize, Serialize};
use tokio::sync::Mutex;

use seatunnel_engine_core::connector_factory::{
    AnySplit, BoxedSinkCommitter, BoxedSinkWriter, BoxedSourceReader,
};
use seatunnel_engine_core::local_checkpoint::{
    CheckpointEnvelope, LocalCheckpointPlan, LocalCheckpointStore, TaskRegistration,
};
use seatunnel_engine_core::task_group::{TaskContext, TaskGroup};

// ---------------------------------------------------------------------------
// Model source: strictly increasing seq, snapshot/restore on the boundary
// ---------------------------------------------------------------------------

struct SeqSource {
    next_seq: u64,
}

#[derive(Serialize, Deserialize)]
struct SeqSourceState {
    next_seq: u64,
}

impl SourceReader for SeqSource {
    type Output = Row;
    type Split = AnySplit;

    fn open(&mut self) -> Pin<Box<dyn Future<Output = anyhow::Result<()>> + Send + '_>> {
        Box::pin(async { Ok(()) })
    }

    fn poll_next(
        &mut self,
    ) -> Pin<Box<dyn Future<Output = anyhow::Result<PollResult<Row>>> + Send + '_>> {
        let seq = self.next_seq;
        self.next_seq += 1;
        Box::pin(async move {
            tokio::time::sleep(Duration::from_millis(1)).await;
            let mut row = Row::new(seatunnel_api::RowKind::Insert, 1);
            row.set(0, Field::Int64(seq as i64));
            Ok(PollResult::Record(row))
        })
    }

    fn snapshot_state(
        &mut self,
    ) -> Pin<Box<dyn Future<Output = anyhow::Result<Vec<u8>>> + Send + '_>> {
        // The record with seq == next_seq-1 may not be committed yet; the
        // safe replay boundary is next_seq-1 ONLY IF the sink's prepare
        // flushed it first — which the engine's barrier order guarantees
        // (prepare_commit runs BEFORE reader.snapshot_state).
        let state = SeqSourceState {
            next_seq: self.next_seq,
        };
        let bytes = serde_json::to_vec(&state).unwrap();
        Box::pin(async move { Ok(bytes) })
    }

    fn add_splits(&mut self, _splits: Vec<Self::Split>) {}
    fn handle_no_more_splits(&mut self) {}

    fn close(&mut self) -> Pin<Box<dyn Future<Output = anyhow::Result<()>> + Send + '_>> {
        Box::pin(async { Ok(()) })
    }
}

impl SeqSource {
    fn restore(&mut self, bytes: &[u8]) {
        if let Ok(state) = serde_json::from_slice::<SeqSourceState>(bytes) {
            self.next_seq = state.next_seq;
        }
    }
}

// ---------------------------------------------------------------------------
// Model exactly-once sink (XA semantics): visible only at phase 2 / settle
// ---------------------------------------------------------------------------

/// Consumer-visible state. `records` grows only through `commit_records`,
/// which advances `watermark` idempotently (the xid analog).
#[derive(Default)]
struct SinkShared {
    records: Vec<u64>,
    watermark: u64,
    /// Set by the writer after each prepare_commit (test fault hook).
    prepare_signal: Option<tokio::sync::mpsc::UnboundedSender<u64>>,
}

impl SinkShared {
    /// Commit a batch of ordered seqs; skip anything at or below the
    /// watermark — replaying a settled window is a no-op (XA COMMIT of an
    /// already-committed xid).
    fn commit_records(&mut self, records: &[u64]) {
        for &seq in records {
            if seq > self.watermark {
                self.records.push(seq);
                self.watermark = seq;
            }
        }
    }
}

#[derive(Serialize, Deserialize)]
struct TestCommitInfo {
    records: Vec<u64>,
}

#[derive(Serialize, Deserialize)]
struct TestWriterState {
    /// Highest seq covered by a prepared (durable) window.
    boundary: u64,
}

struct TestSink {
    shared: Arc<Mutex<SinkShared>>,
    buffer: Vec<u64>,
    /// Prepared-but-not-yet-phase-2 window (kept so close() can settle it).
    prepared: Vec<u64>,
    boundary: u64,
    prepare_count: u64,
}

impl TestSink {
    fn new(shared: Arc<Mutex<SinkShared>>) -> Self {
        TestSink {
            shared,
            buffer: Vec::new(),
            prepared: Vec::new(),
            boundary: 0,
            prepare_count: 0,
        }
    }

    /// Restore from the envelope: writer state sets the boundary; the
    /// persisted commit infos are settled into the output (the in-memory
    /// analog of XA RECOVER committing prepared xids).
    async fn restore(&mut self, state: &[u8], commit_infos: &[Vec<u8>]) {
        if let Ok(state) = serde_json::from_slice::<TestWriterState>(state) {
            self.boundary = state.boundary;
        }
        let mut shared = self.shared.lock().await;
        for info in commit_infos {
            if let Ok(info) = serde_json::from_slice::<TestCommitInfo>(info) {
                shared.commit_records(&info.records);
            }
        }
    }
}

impl SinkWriter for TestSink {
    type Input = Row;
    type WriterState = Vec<u8>;
    type CommitInfo = Vec<u8>;

    fn open(&mut self) -> Pin<Box<dyn Future<Output = anyhow::Result<()>> + Send + '_>> {
        Box::pin(async { Ok(()) })
    }

    fn write(
        &mut self,
        record: Self::Input,
    ) -> Pin<Box<dyn Future<Output = anyhow::Result<()>> + Send + '_>> {
        let seq = match record.fields.first() {
            Some(Field::Int64(v)) => *v as u64,
            _ => 0,
        };
        self.buffer.push(seq);
        Box::pin(async { Ok(()) })
    }

    fn prepare_commit(
        &mut self,
        _checkpoint_id: u64,
    ) -> Pin<Box<dyn Future<Output = anyhow::Result<Vec<Self::CommitInfo>>> + Send + '_>> {
        // Phase 1: the window becomes durable (prepared) — records move out
        // of the live buffer into the commit descriptor.
        let mut records = std::mem::take(&mut self.buffer);
        let prepared = std::mem::take(&mut self.prepared);
        records.extend(prepared);
        records.sort_unstable();
        if let Some(&last) = records.last() {
            self.boundary = last;
        }
        self.prepared = records.clone();
        self.prepare_count += 1;
        let count = self.prepare_count;
        let shared = Arc::clone(&self.shared);
        Box::pin(async move {
            if let Some(signal) = &shared.lock().await.prepare_signal {
                let _ = signal.send(count);
            }
            let info = serde_json::to_vec(&TestCommitInfo { records }).unwrap();
            Ok(vec![info])
        })
    }

    fn snapshot_state(
        &mut self,
    ) -> Pin<Box<dyn Future<Output = anyhow::Result<Vec<u8>>> + Send + '_>> {
        let state = serde_json::to_vec(&TestWriterState {
            boundary: self.boundary,
        })
        .unwrap();
        Box::pin(async move { Ok(state) })
    }

    fn poll_flush(&mut self) -> Pin<Box<dyn Future<Output = anyhow::Result<()>> + Send + '_>> {
        Box::pin(async { Ok(()) })
    }

    fn close(&mut self) -> Pin<Box<dyn Future<Output = anyhow::Result<()>> + Send + '_>> {
        // Settle whatever is still prepared (tail without a checkpoint).
        let prepared = std::mem::take(&mut self.prepared);
        let shared = Arc::clone(&self.shared);
        Box::pin(async move {
            shared.lock().await.commit_records(&prepared);
            Ok(())
        })
    }
}

struct TestCommitter {
    shared: Arc<Mutex<SinkShared>>,
}

impl SinkCommitter for TestCommitter {
    type CommitInfo = Vec<u8>;
    type AggregatedCommitInfo = serde_json::Value;

    fn commit(
        &mut self,
        commit_infos: Vec<Self::CommitInfo>,
    ) -> CommitterFuture<'_, Self::AggregatedCommitInfo> {
        let shared = Arc::clone(&self.shared);
        Box::pin(async move {
            let mut guard = shared.lock().await;
            let mut committed = 0usize;
            for info in commit_infos {
                if let Ok(info) = serde_json::from_slice::<TestCommitInfo>(&info) {
                    committed += info.records.len();
                    guard.commit_records(&info.records);
                }
            }
            Ok(serde_json::json!({ "committed": committed }))
        })
    }

    fn abort(&mut self, _commit_infos: Vec<Self::CommitInfo>) -> CommitterFuture<'_, ()> {
        // The engine aborted this checkpoint: the prepared window dies with
        // the (crashed) writer in a real deployment; nothing to do here.
        Box::pin(async { Ok(()) })
    }
}

// ---------------------------------------------------------------------------
// Session harness
// ---------------------------------------------------------------------------

enum Fault {
    /// Kill right after the sink's Nth prepare_commit (phase 1 done, most
    /// likely before the envelope is persisted).
    AfterPhase1(u64),
    /// Kill right after a checkpoint newer than `than` appears on disk
    /// (persisted; phase 2 may not have run yet).
    AfterPersist(u64),
    /// Kill after a fixed delay, no checkpoint coordination involved.
    MidStream(Duration),
    /// Graceful shutdown (final checkpoint).
    Graceful,
}

struct SessionResult {
    last_envelope: Option<CheckpointEnvelope>,
}

/// One process lifetime: build plan (restoring from the store), run the
/// task + driver, then die according to `fault`.
async fn run_session(
    state_root: &std::path::Path,
    job_id: &str,
    shared: Arc<Mutex<SinkShared>>,
    fault: Fault,
) -> SessionResult {
    let store = LocalCheckpointStore::new(state_root);
    let mut plan = LocalCheckpointPlan::new(state_root, job_id, Duration::from_millis(30))
        .restore_from_latest()
        .unwrap();
    let envelope = plan.restore_envelope().cloned();

    // Arm the fault hook: the sink signals every prepare_commit.
    let (prepare_tx, mut prepare_rx) = tokio::sync::mpsc::unbounded_channel();
    {
        let mut guard = shared.lock().await;
        guard.prepare_signal = Some(prepare_tx);
    }

    let mut source = SeqSource { next_seq: 1 };
    let mut sink = TestSink::new(Arc::clone(&shared));
    if let Some(envelope) = &envelope {
        if let Some(task_state) = envelope.task_state("p0", 0) {
            source.restore(&task_state.reader_state);
            sink.restore(&task_state.writer_state, &task_state.commit_infos)
                .await;
        }
    }

    let handle = plan.register(TaskRegistration {
        task_id: "job-p0-local-0".to_string(),
        pipeline: "p0".to_string(),
        subtask: 0,
        parallelism: 1,
    });
    let cancel = Arc::new(tokio_util::sync::CancellationToken::new());
    let shutdown = tokio_util::sync::CancellationToken::new();

    let context = TaskContext::new("job-p0-local-0", job_id, "p0", 0, 1)
        .with_cancel_token(Arc::clone(&cancel))
        .with_checkpoint_handle(handle);
    let reader: BoxedSourceReader = Box::new(source);
    let writer: BoxedSinkWriter = Box::new(sink);
    let committer: Option<BoxedSinkCommitter> = Some(Box::new(TestCommitter {
        shared: Arc::clone(&shared),
    }));
    let task_join = tokio::spawn(async move {
        TaskGroup::new(context, reader, writer)
            .with_committer(committer)
            .run()
            .await
    });
    let driver_join = tokio::spawn(plan.build().run(shutdown.clone(), Arc::clone(&cancel)));

    // Drive until the fault condition is met.
    match fault {
        Fault::AfterPhase1(n) => {
            while let Some(count) = prepare_rx.recv().await {
                if count >= n {
                    break;
                }
            }
        }
        Fault::AfterPersist(than) => {
            loop {
                if let Some(envelope) = store.load_latest(job_id).unwrap() {
                    if envelope.checkpoint_id > than {
                        break;
                    }
                }
                // Keep the prepare channel drained.
                let _ = prepare_rx.try_recv();
                tokio::time::sleep(Duration::from_millis(2)).await;
            }
        }
        Fault::MidStream(delay) => {
            tokio::time::sleep(delay).await;
        }
        Fault::Graceful => {
            shutdown.cancel();
            let _ = tokio::time::timeout(Duration::from_secs(30), task_join).await;
            let _ = tokio::time::timeout(Duration::from_secs(30), driver_join).await;
            {
                let mut guard = shared.lock().await;
                guard.prepare_signal = None;
            }
            return SessionResult {
                last_envelope: store.load_latest(job_id).unwrap(),
            };
        }
    }

    // Simulated kill -9: no final checkpoint, no close, no shutdown signal.
    task_join.abort();
    driver_join.abort();
    let _ = tokio::time::timeout(Duration::from_secs(5), task_join).await;
    let _ = tokio::time::timeout(Duration::from_secs(5), driver_join).await;
    {
        let mut guard = shared.lock().await;
        guard.prepare_signal = None;
    }
    SessionResult {
        last_envelope: store.load_latest(job_id).unwrap(),
    }
}

fn temp_root(tag: &str) -> std::path::PathBuf {
    let root = std::env::temp_dir().join(format!("st-cp-recovery-{}-{}", tag, std::process::id()));
    let _ = std::fs::remove_dir_all(&root);
    std::fs::create_dir_all(&root).unwrap();
    root
}

/// The core assertion: after the whole crash matrix, the consumer-visible
/// output is exactly 1..=watermark with no duplicates and no gaps.
fn assert_exactly_once(shared: &SinkShared, min_committed: u64) {
    assert!(
        shared.watermark >= min_committed,
        "expected at least {} committed records, got watermark {}",
        min_committed,
        shared.watermark
    );
    let expected: Vec<u64> = (1..=shared.watermark).collect();
    assert_eq!(
        shared.records, expected,
        "output must be exactly 1..={} with no duplicates or gaps",
        shared.watermark
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn exactly_once_across_crash_matrix() {
    let root = temp_root("matrix");
    let shared = Arc::new(Mutex::new(SinkShared::default()));
    let job = "recovery-job";

    // Session A: die right after the 3rd phase-1.
    let result = run_session(&root, job, Arc::clone(&shared), Fault::AfterPhase1(3)).await;
    let after_a = result.last_envelope.map(|e| e.checkpoint_id).unwrap_or(0);

    // Session B: die right after a NEW envelope is persisted.
    let result = run_session(
        &root,
        job,
        Arc::clone(&shared),
        Fault::AfterPersist(after_a),
    )
    .await;
    let after_b = result
        .last_envelope
        .map(|e| e.checkpoint_id)
        .unwrap_or(after_a);

    // Session C: die mid-stream with nothing coordinated.
    let result = run_session(
        &root,
        job,
        Arc::clone(&shared),
        Fault::MidStream(Duration::from_millis(150)),
    )
    .await;
    let _ = result;

    // Session D: graceful shutdown (final checkpoint + tail settle).
    let result = run_session(&root, job, Arc::clone(&shared), Fault::Graceful).await;
    let final_envelope = result
        .last_envelope
        .expect("final session must persist a checkpoint");
    assert!(final_envelope.is_final, "shutdown checkpoint must be final");
    assert!(final_envelope.checkpoint_id > after_b);

    let guard = shared.lock().await;
    // Across three kill -9 sessions plus a graceful one, with 1ms records
    // and 30ms checkpoints, dozens of records must have flowed.
    assert_exactly_once(&guard, 20);
    let _ = std::fs::remove_dir_all(&root);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn exactly_once_with_repeated_phase1_crashes() {
    // Crash repeatedly at the same protocol point: only phase-1 completions
    // with NO persisted envelope — the worst duplicate window.
    let root = temp_root("phase1");
    let shared = Arc::new(Mutex::new(SinkShared::default()));
    let job = "phase1-job";

    for i in 0..5 {
        // Crash after the FIRST prepare of each session: nothing persisted
        // for this session yet (envelope from previous sessions only).
        run_session(&root, job, Arc::clone(&shared), Fault::AfterPhase1(1)).await;
        // Give the store a moment so the next session's AfterPersist-style
        // bookkeeping stays deterministic.
        tokio::time::sleep(Duration::from_millis(20)).await;
        let _ = i;
    }
    run_session(&root, job, Arc::clone(&shared), Fault::Graceful).await;

    let guard = shared.lock().await;
    assert_exactly_once(&guard, 5);
    let _ = std::fs::remove_dir_all(&root);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn exactly_once_crashing_right_after_persist() {
    // Crash over and over exactly between envelope persistence and phase 2.
    let root = temp_root("persist");
    let shared = Arc::new(Mutex::new(SinkShared::default()));
    let job = "persist-job";

    let mut than = 0u64;
    for _ in 0..4 {
        let result = run_session(&root, job, Arc::clone(&shared), Fault::AfterPersist(than)).await;
        than = result
            .last_envelope
            .map(|e| e.checkpoint_id)
            .unwrap_or(than);
    }
    run_session(&root, job, Arc::clone(&shared), Fault::Graceful).await;

    let guard = shared.lock().await;
    assert_exactly_once(&guard, 5);
    let _ = std::fs::remove_dir_all(&root);
}
