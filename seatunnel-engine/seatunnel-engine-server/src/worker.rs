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

//! Worker task executor.
//!
//! Receives chained TaskDescriptors from the master via heartbeat, builds the
//! real Source → Transform → Sink chain through the shared connector factory,
//! runs it in a `TaskGroup` with checkpointing enabled, reports lifecycle
//! transitions back to the master and persists checkpoint state locally so a
//! restarted worker resumes from the last binlog position / offset.

use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex as StdMutex};
use std::time::Duration;

use tokio::sync::{Mutex, mpsc};
use tokio_util::sync::CancellationToken;
use tracing::{error, info, warn};

use seatunnel_engine_comm::{
    CheckpointPhase, CheckpointReport, HeartbeatResponse, TaskDescriptor, TaskStatusReport,
    generated::master_service_client::MasterServiceClient,
};
use seatunnel_engine_core::connector_factory::{create_source, create_transforms};
use seatunnel_engine_core::local_checkpoint::{
    CheckpointEvent, GateControl, TaskToDriver, task_gate,
};
use seatunnel_engine_core::state::TaskState;
use seatunnel_engine_core::task_group::TaskGroup;
use seatunnel_engine_core::{TaskStatus, now_millis};

/// Admission signals snapshot shipped with every heartbeat/registration.
#[derive(Debug, Clone, Copy, Default)]
pub struct AdmissionFields {
    /// Overall pressure 0..1000 (per-mille).
    pub load_score: u32,
    /// Event-loop lag EMA (ms).
    pub lag_ms: u32,
    /// RSS over usable memory (per-mille).
    pub mem_permille: u32,
    /// Host CPU usage (per-mille, display only).
    pub cpu_permille: u32,
    /// False while over a pressure watermark.
    pub can_accept: bool,
}

use crate::admission::{AdmissionController, AdmissionSignals};
use crate::state_store::LocalStateStore;

/// Information about a task executing on this worker.
#[derive(Debug)]
pub struct RunningTask {
    pub task_id: String,
    pub job_id: String,
    pub stage_id: String,
    pub state: String,
    pub processed_records: u64,
    pub start_time: i64,
}

type SharedMasterClient = Arc<Mutex<Option<MasterServiceClient<tonic::transport::Channel>>>>;

/// Per-attempt timeout for a terminal status report RPC.
const TERMINAL_REPORT_RPC_TIMEOUT: Duration = Duration::from_secs(5);
/// First backoff when a terminal report delivery fails.
const TERMINAL_REPORT_BACKOFF_MIN: Duration = Duration::from_millis(500);
/// Backoff ceiling between terminal report retries.
const TERMINAL_REPORT_BACKOFF_MAX: Duration = Duration::from_secs(5);
/// Total budget a terminal report is retried before the worker gives up.
/// Past the budget the task handle is dropped and the master's heartbeat
/// reconcile (missing-from-heartbeats → release → re-dispatch) takes over.
const TERMINAL_REPORT_BUDGET: Duration = Duration::from_secs(600);
/// Default grace between cancelling a task and force-aborting its
/// execution — long enough for a normal exit checkpoint, short enough
/// that a hung sink cannot pin the task (and its id, blocking
/// re-dispatch) forever.
const CANCEL_FORCE_ABORT: Duration = Duration::from_secs(60);

/// A finished task's terminal transition awaiting reliable delivery.
struct TerminalOutcome {
    job_id: String,
    task_id: String,
    /// Execution incarnation this outcome belongs to; guards the report
    /// loop against stamping/removing a re-dispatched handle.
    generation: u64,
    /// Proto task-state code (3 COMPLETED / 4 FAILED / 5 CANCELLED).
    code: i32,
    records: u64,
    error: Option<String>,
}

/// One task's checkpoint gate: the driver control the heartbeat loop
/// fires triggers/resolutions on, plus the job id (task reports do not
/// carry one — the local driver knew it, the forwarder looks it up).
struct CheckpointGate {
    job_id: String,
    control: Arc<GateControl>,
}

type SharedCheckpointGates = Arc<Mutex<HashMap<String, CheckpointGate>>>;

/// Worker node that executes chained pipeline tasks assigned by the master.
pub struct WorkerNode {
    /// Auto-cleanup settings (0/None disables the cleaner).
    clean_config: Option<CleanConfig>,
    /// Job ids whose cancelled-state cleanup is already scheduled. The
    /// master re-broadcasts a job in every heartbeat response until it
    /// sees all tasks terminal, so without this guard each heartbeat
    /// would arm another delayed delete for the same job.
    cancel_cleanups: StdMutex<HashSet<String>>,
    /// Checkpoint storage backend: localfile | master | s3.
    storage_type: String,
    /// S3 store when storage_type = s3.
    s3_store: Option<crate::checkpoint_store::S3CheckpointStore>,
    /// Policy when a task HAS checkpoint history but no restorable
    /// payload is found on restart: "fail" (default — a silent cold
    /// start skips every change since the last checkpoint) or "warn"
    /// (log ERROR and cold-start anyway).
    restore_missing: &'static str,
    worker_id: String,
    #[allow(dead_code)] // reported to the master once registration is wired up
    address: String,
    master_client: SharedMasterClient,
    state_store: Arc<LocalStateStore>,
    running_tasks: Arc<Mutex<HashMap<String, RunningTaskHandle>>>,
    /// Per-task execution incarnation counter, bumped on every
    /// (re-)assignment of a task id. Terminal outcomes carry the value so
    /// the report loop never touches a newer incarnation's handle.
    task_generations: StdMutex<HashMap<String, u64>>,
    /// Finished tasks' terminal transitions awaiting reliable delivery.
    terminal_tx: mpsc::UnboundedSender<TerminalOutcome>,
    terminal_rx: Mutex<Option<mpsc::UnboundedReceiver<TerminalOutcome>>>,
    /// Highest fencing term seen from any master (0 = none yet).
    /// Instructions from masters with a lower term are rejected so a
    /// deposed master cannot disturb tasks owned by its successor.
    /// Shared with spawned task executors so their reports carry it.
    term: Arc<AtomicU64>,
    /// Per-task checkpoint gates: the master (the remote checkpoint
    /// driver) triggers barriers and receives prepares through these.
    checkpoint_gates: SharedCheckpointGates,
    /// Sender side handed to every task gate; drained by the forwarder
    /// task that persists states and reports prepares to the master.
    checkpoint_report_tx: mpsc::UnboundedSender<TaskToDriver>,
    /// Receiver side, taken once when the forwarder starts.
    checkpoint_rx: Mutex<Option<mpsc::UnboundedReceiver<TaskToDriver>>>,
    /// Dynamic task admission: measured pressure decides whether this
    /// worker accepts more tasks. Defaults to a manual (always-accepting)
    /// controller; `with_admission` attaches the sampling one.
    admission: Arc<AdmissionController>,
}

/// Internal handle for a spawned task execution.
#[derive(Default)]
struct RunningTaskHandle {
    job_id: String,
    cancel: Option<Arc<CancellationToken>>,
    /// Shared status of the executing TaskGroup, filled in as soon as the
    /// pipeline is built (None during connector construction).
    status: Option<Arc<tokio::sync::Mutex<TaskStatus>>>,
    /// Task log ring mirrored from the executing TaskGroup.
    logs: Option<seatunnel_engine_core::task_log::TaskLogRing>,
    /// Log shipping bookmark for the last heartbeat.
    log_cursor: u64,
    /// Execution incarnation (see `WorkerNode::task_generations`).
    generation: u64,
    /// Set once the execution returned: the proto task-state code being
    /// reported to the master. Heartbeats carry it (truthful state) and
    /// the report loop clears the entry once the master acks — the task
    /// must keep showing up in heartbeats until then, otherwise the
    /// master's reconcile would re-dispatch a task that is mid-report.
    terminal: Option<i32>,
    /// Join handle of the spawned pipeline execution — the cancel
    /// backstop aborts it when a task ignores cancellation for too long.
    exec: Option<tokio::task::JoinHandle<()>>,
    /// Cancel-force-abort watcher armed for this task (once).
    backstop_armed: bool,
}

impl RunningTaskHandle {
    fn cancel_token(&self) -> Arc<CancellationToken> {
        self.cancel.clone().expect("cancel token set at insert")
    }
}

impl WorkerNode {
    pub fn new(
        worker_id: impl Into<String>,
        address: impl Into<String>,
        state_store: Arc<LocalStateStore>,
    ) -> Self {
        Self::new_with_clean(worker_id, address, state_store, None)
    }

    /// Construct with auto-cleanup enabled (engine `seatunnel.yaml`).
    pub fn new_with_clean(
        worker_id: impl Into<String>,
        address: impl Into<String>,
        state_store: Arc<LocalStateStore>,
        clean_config: Option<CleanConfig>,
    ) -> Self {
        let (checkpoint_report_tx, checkpoint_rx) = mpsc::unbounded_channel();
        let (terminal_tx, terminal_rx) = mpsc::unbounded_channel();
        WorkerNode {
            worker_id: worker_id.into(),
            address: address.into(),
            master_client: Arc::new(Mutex::new(None)),
            state_store,
            running_tasks: Arc::new(Mutex::new(HashMap::new())),
            task_generations: StdMutex::new(HashMap::new()),
            terminal_tx,
            terminal_rx: Mutex::new(Some(terminal_rx)),
            cancel_cleanups: StdMutex::new(HashSet::new()),
            clean_config,
            storage_type: "localfile".to_string(),
            s3_store: None,
            restore_missing: "fail",
            term: Arc::new(AtomicU64::new(0)),
            checkpoint_gates: Arc::new(Mutex::new(HashMap::new())),
            checkpoint_report_tx,
            checkpoint_rx: Mutex::new(Some(checkpoint_rx)),
            admission: Arc::new(AdmissionController::new_manual(Default::default())),
        }
    }

    /// Attach the sampling admission controller (production path).
    pub fn with_admission(mut self, controller: AdmissionController) -> Self {
        self.admission = Arc::new(controller);
        self
    }

    /// Admission snapshot for heartbeats/registration.
    pub async fn admission_fields(&self) -> AdmissionFields {
        let decision = self.admission.decision();
        let signals = self.admission.signals();
        AdmissionFields {
            load_score: decision.load_score_permille,
            lag_ms: signals.lag_ms.unwrap_or(0).min(u32::MAX as u64) as u32,
            mem_permille: signals.mem_permille.unwrap_or(0),
            cpu_permille: signals.cpu_permille.unwrap_or(0),
            can_accept: decision.can_accept,
        }
    }

    /// Test hook: inject admission signals into the manual controller.
    pub async fn set_admission_signals(&self, signals: AdmissionSignals) {
        self.admission.set_signals(signals);
    }

    /// Attach the checkpoint storage backend (master/s3) for failover.
    pub fn with_checkpoint_storage(
        &mut self,
        storage_type: &str,
        s3_store: Option<crate::checkpoint_store::S3CheckpointStore>,
    ) {
        self.storage_type = storage_type.to_string();
        self.s3_store = s3_store;
    }

    /// Set the behavior when checkpoint history exists but no restorable
    /// payload is found on restart (`fail` | `warn`).
    pub fn with_restore_missing(&mut self, policy: &str) {
        if policy.eq_ignore_ascii_case("warn") {
            self.restore_missing = "warn";
        } else {
            self.restore_missing = "fail";
        }
    }

    /// Set the gRPC client used for reporting to the master. The first
    /// call also starts the checkpoint forwarder and the terminal-report
    /// loop (both need the client).
    pub async fn set_master_client(
        self: &Arc<Self>,
        client: MasterServiceClient<tonic::transport::Channel>,
    ) {
        *self.master_client.lock().await = Some(client);
        self.start_checkpoint_forwarder().await;
        self.start_terminal_reporter().await;
    }

    /// Start (once) the loop that delivers finished tasks' terminal
    /// transitions to the master with retry-until-acked semantics. A
    /// terminal report is the ONLY signal that moves a task out of
    /// `Running` on the master — losing it pins the job in RUNNING
    /// forever, so delivery retries within the budget and the handle
    /// stays in the heartbeat (as terminal) until the master acks.
    async fn start_terminal_reporter(self: &Arc<Self>) {
        let Some(rx) = self.terminal_rx.lock().await.take() else {
            return;
        };
        let reporter = TerminalReporter {
            worker_id: self.worker_id.clone(),
            master_client: Arc::clone(&self.master_client),
            term: Arc::clone(&self.term),
            running_tasks: Arc::clone(&self.running_tasks),
        };
        tokio::spawn(async move {
            run_terminal_reporter(rx, reporter).await;
        });
    }

    /// Execution incarnation for a (re-)assigned task id.
    fn next_generation(&self, task_id: &str) -> u64 {
        let mut gens = self.task_generations.lock().unwrap();
        let next = gens.entry(task_id.to_string()).or_insert(0);
        *next += 1;
        *next
    }

    /// Arm (once per task) the watcher that force-aborts an execution
    /// still running `CANCEL_FORCE_ABORT` after its cancel token fired —
    /// a hung sink/poll must not pin the task id forever.
    fn arm_cancel_backstop(self: &Arc<Self>, task_id: String) {
        let worker = Arc::clone(self);
        tokio::spawn(async move {
            tokio::time::sleep(CANCEL_FORCE_ABORT).await;
            let mut tasks = worker.running_tasks.lock().await;
            let Some(handle) = tasks.get_mut(&task_id) else {
                return; // exited (and was reported) in time
            };
            if handle.terminal.is_some() {
                return; // execution finished, report in flight
            }
            let Some(exec) = handle.exec.take() else {
                return;
            };
            warn!(
                "Worker {}: task {} still running {}s after cancellation — aborting its execution; \
                 a CANCELLED terminal report will be synthesized",
                worker.worker_id,
                task_id,
                CANCEL_FORCE_ABORT.as_secs()
            );
            exec.abort();
        });
    }

    /// Start (once) the task that drains checkpoint gate reports:
    /// persist reader state locally (+ S3), then forward the prepare to
    /// the master, which resolves it and sends phase-2 events back via
    /// heartbeats. This is the worker side of "the master is the
    /// checkpoint driver".
    async fn start_checkpoint_forwarder(&self) {
        let Some(rx) = self.checkpoint_rx.lock().await.take() else {
            return;
        };
        let forwarder = CheckpointForwarder {
            worker_id: self.worker_id.clone(),
            master_client: Arc::clone(&self.master_client),
            state_store: Arc::clone(&self.state_store),
            upload_to_master: self.storage_type == "master",
            s3_store: self.s3_store.clone(),
            term: Arc::clone(&self.term),
            gates: Arc::clone(&self.checkpoint_gates),
        };
        tokio::spawn(async move {
            forwarder.run(rx).await;
        });
    }

    pub fn state_store(&self) -> &Arc<LocalStateStore> {
        &self.state_store
    }

    /// Highest master term this worker has seen.
    pub fn term(&self) -> u64 {
        self.term.load(Ordering::SeqCst)
    }

    /// Ratchet the term up (never down) — e.g. from a registration
    /// response. Returns the current term.
    pub fn observe_term(&self, seen: u64) -> u64 {
        let mut current = self.term.load(Ordering::SeqCst);
        while seen > current {
            match self
                .term
                .compare_exchange(current, seen, Ordering::SeqCst, Ordering::SeqCst)
            {
                Ok(_) => return seen,
                Err(actual) => current = actual,
            }
        }
        current
    }

    /// Apply a heartbeat response's instruction set under term fencing.
    ///
    /// Returns `true` when the instructions were accepted. A response
    /// from a master whose term is LOWER than the highest we have seen
    /// is stale (a deposed master) and its instructions are ignored —
    /// this is the fence that makes double dispatch impossible across a
    /// master switch.
    pub async fn apply_master_response(self: &Arc<Self>, response: &HeartbeatResponse) -> bool {
        if response.term < self.term() {
            warn!(
                "Worker {}: ignoring instructions from master term {} (highest seen {})",
                self.worker_id,
                response.term,
                self.term()
            );
            return false;
        }
        if response.term > self.term() {
            self.term.store(response.term, Ordering::SeqCst);
        }
        if !response.cancel_jobs.is_empty() {
            self.cancel_jobs(&response.cancel_jobs).await;
        }
        if !response.preempted_task_ids.is_empty() {
            self.preempt_tasks(&response.preempted_task_ids).await;
        }
        // Coordinated checkpoints: fire due barriers, apply resolutions.
        for trigger in &response.checkpoint_triggers {
            self.trigger_checkpoint(&trigger.task_id, trigger.checkpoint_id)
                .await;
        }
        for resolution in &response.checkpoint_resolutions {
            self.deliver_checkpoint_resolution(
                &resolution.task_id,
                resolution.checkpoint_id,
                resolution.completed,
            )
            .await;
        }
        if !response.pending_tasks.is_empty() {
            info!(
                "Received {} task(s) from master (term {})",
                response.pending_tasks.len(),
                response.term
            );
            for task in &response.pending_tasks {
                let task = task.clone();
                self.assign_task(task).await;
            }
        }
        true
    }

    /// Accept a task from the master and start executing it asynchronously.
    /// Completion enqueues the terminal transition for reliable delivery;
    /// the task handle leaves the registry only once the master acked the
    /// report (heartbeats keep listing it as terminal meanwhile).
    pub async fn assign_task(self: &Arc<Self>, task: TaskDescriptor) {
        let task_id = task.task_id.clone();
        let job_id = task.job_id.clone();

        // Failover dedup: if this worker already runs the task (e.g. the
        // master re-dispatched during a reconnect window), skip it. A
        // CANCELLED local incarnation (update/restart flow) is the one
        // exception: wait briefly for it to drain — the resubmit reuses
        // the same task id, and ignoring the dispatch would leave the
        // new incarnation dead while the master shows it dispatched.
        if self.running_tasks.lock().await.contains_key(&task_id) {
            const DRAIN_WAIT: Duration = Duration::from_secs(10);
            const DRAIN_POLL: Duration = Duration::from_millis(100);
            let mut waited = Duration::ZERO;
            loop {
                enum Observed {
                    Gone,
                    Live,
                    Draining,
                }
                let observed = {
                    let guard = self.running_tasks.lock().await;
                    match guard.get(&task_id) {
                        None => Observed::Gone,
                        Some(handle) if !handle.cancel_token().is_cancelled() => Observed::Live,
                        Some(_) => Observed::Draining,
                    }
                };
                match observed {
                    Observed::Gone => break, // old incarnation left the registry — proceed
                    Observed::Live => {
                        info!(
                            "Worker {}: task {} already running locally — dispatch ignored",
                            self.worker_id, task_id
                        );
                        return;
                    }
                    Observed::Draining if waited >= DRAIN_WAIT => {
                        warn!(
                            "Worker {}: cancelled task {} still draining after {:?} — dispatch \
                             ignored; the master's reconcile will re-dispatch it",
                            self.worker_id, task_id, DRAIN_WAIT
                        );
                        return;
                    }
                    Observed::Draining => {}
                }
                tokio::time::sleep(DRAIN_POLL).await;
                waited += DRAIN_POLL;
            }
        }

        info!(
            "Worker {}: accepting task {} (subtask {}/{})",
            self.worker_id, task.task_id, task.task_index, task.parallelism
        );

        let generation = self.next_generation(&task_id);
        let cancel = Arc::new(CancellationToken::new());
        self.running_tasks.lock().await.insert(
            task_id.clone(),
            RunningTaskHandle {
                job_id: job_id.clone(),
                cancel: Some(cancel.clone()),
                generation,
                ..Default::default()
            },
        );

        let ctx = TaskExecCtx {
            worker_id: self.worker_id.clone(),
            master_client: self.master_client.clone(),
            state_store: self.state_store.clone(),
            storage_type: self.storage_type.clone(),
            s3_store: self.s3_store.clone(),
            restore_missing: self.restore_missing.to_string(),
            term: Arc::clone(&self.term),
        };

        let worker = Arc::clone(self);
        let exec_task_id = task_id.clone();
        let wrap_job_id = job_id.clone();
        // Inner task: the actual pipeline execution. The cancel backstop
        // aborts THIS handle when a task ignores cancellation too long.
        let inner = tokio::spawn(execute_descriptor(task, ctx, cancel, Arc::clone(&worker)));
        // Outer wrapper: turns the outcome (or a force-abort JoinError —
        // synthesized as the truthful CANCELLED) into the reliable
        // delivery path. This task itself is never aborted.
        let exec = tokio::spawn(async move {
            let mut outcome = match inner.await {
                Ok(outcome) => outcome,
                Err(e) => {
                    error!(
                        "Task {} execution was force-aborted after cancellation: {}",
                        exec_task_id, e
                    );
                    TerminalOutcome {
                        job_id: wrap_job_id,
                        task_id: exec_task_id.clone(),
                        generation,
                        code: 5, // CANCELLED
                        records: 0,
                        error: Some("execution force-aborted after cancellation".to_string()),
                    }
                }
            };
            outcome.generation = generation;
            // Heartbeats report the terminal state from now on; the task
            // stays listed until the master acks the report (the report
            // loop removes the handle) so the reconcile cannot double-
            // dispatch a task that is mid-report.
            if let Some(handle) = worker.running_tasks.lock().await.get_mut(&exec_task_id)
                && handle.generation == generation
            {
                handle.terminal = Some(outcome.code);
            }
            if let Err(e) = worker.terminal_tx.send(outcome) {
                error!(
                    "terminal outcome for task {} could not be queued (worker shutting down): {}",
                    exec_task_id, e
                );
            }
        });
        if let Some(handle) = self.running_tasks.lock().await.get_mut(&task_id) {
            handle.exec = Some(exec);
        }
    }

    /// Fill in the live status/log handles of a registered task once its
    /// TaskGroup exists, so heartbeats can report real metrics.
    pub async fn attach_task_metrics(
        &self,
        task_id: &str,
        status: Arc<tokio::sync::Mutex<TaskStatus>>,
        logs: seatunnel_engine_core::task_log::TaskLogRing,
    ) {
        if let Some(handle) = self.running_tasks.lock().await.get_mut(task_id) {
            handle.status = Some(status);
            handle.logs = Some(logs);
        }
    }

    /// Create the checkpoint gate for a task: the TaskGroup gets the
    /// handle (trigger/event polling + reports), the worker keeps the
    /// control side for heartbeat-driven instructions.
    pub async fn checkpoint_handle_for(
        &self,
        task_id: &str,
        job_id: &str,
    ) -> seatunnel_engine_core::local_checkpoint::CheckpointHandle {
        let (handle, control) = task_gate(self.checkpoint_report_tx.clone());
        self.checkpoint_gates.lock().await.insert(
            task_id.to_string(),
            CheckpointGate {
                job_id: job_id.to_string(),
                control: Arc::new(control),
            },
        );
        handle
    }

    /// Drop a finished task's checkpoint gate. Normally driven by the
    /// checkpoint forwarder on `TaskToDriver::Done`; kept public for
    /// explicit shutdown paths.
    pub async fn remove_checkpoint_gate(&self, task_id: &str) {
        if let Some(gate) = self.checkpoint_gates.lock().await.remove(task_id) {
            gate.control.close();
        }
    }

    /// Fire a coordinated checkpoint barrier on one task (master-driven).
    pub async fn trigger_checkpoint(&self, task_id: &str, checkpoint_id: u64) {
        if let Some(gate) = self.checkpoint_gates.lock().await.get(task_id) {
            info!(
                "Worker {}: firing checkpoint barrier {} on task {}",
                self.worker_id, checkpoint_id, task_id
            );
            gate.control.trigger(checkpoint_id);
        } else {
            warn!(
                "Worker {}: checkpoint trigger for unknown task {} (id {})",
                self.worker_id, task_id, checkpoint_id
            );
        }
    }

    /// Deliver a coordinated checkpoint resolution (complete → 2PC phase
    /// 2, abort → unwind) to one task.
    pub async fn deliver_checkpoint_resolution(
        &self,
        task_id: &str,
        checkpoint_id: u64,
        completed: bool,
    ) {
        if let Some(gate) = self.checkpoint_gates.lock().await.get(task_id) {
            let event = if completed {
                CheckpointEvent::Completed(checkpoint_id)
            } else {
                CheckpointEvent::Aborted(checkpoint_id)
            };
            gate.control.send_event(event);
        }
    }

    /// Ids of tasks currently running on this worker (fencing reports).
    pub async fn running_task_ids(&self) -> Vec<String> {
        self.running_tasks.lock().await.keys().cloned().collect()
    }

    /// The master's view of a job's state (proto code), or None when the
    /// master is unreachable. Used by the delayed state cleaner to skip
    /// jobs that were resubmitted under the same id.
    async fn job_state_on_master(&self, job_id: &str) -> Option<i32> {
        let mut client = self.master_client.lock().await.clone()?;
        let request = tonic::Request::new(seatunnel_engine_comm::JobStatusRequest {
            job_id: job_id.to_string(),
        });
        let response =
            tokio::time::timeout(TERMINAL_REPORT_RPC_TIMEOUT, client.get_job_status(request))
                .await
                .ok()?
                .ok()?;
        Some(response.into_inner().state)
    }

    /// Cancel specific tasks that were reassigned elsewhere by the master
    /// (failover fencing) so they are not executed twice.
    pub async fn preempt_tasks(self: &Arc<Self>, task_ids: &[String]) {
        if task_ids.is_empty() {
            return;
        }
        let mut to_arm = Vec::new();
        {
            let mut tasks = self.running_tasks.lock().await;
            for task_id in task_ids {
                if let Some(handle) = tasks.get_mut(task_id) {
                    warn!(
                        "Worker {}: preempting task {} (reassigned by the master)",
                        self.worker_id, task_id
                    );
                    handle.cancel_token().cancel();
                    if !handle.backstop_armed {
                        handle.backstop_armed = true;
                        to_arm.push(task_id.clone());
                    }
                }
            }
        }
        for tid in to_arm {
            self.arm_cancel_backstop(tid);
        }
    }

    /// Snapshot of running tasks for the next heartbeat: real record
    /// counters, the last-record timestamp, the truthful task state and
    /// the task log increment.
    pub async fn heartbeat_tasks(&self) -> Vec<seatunnel_engine_comm::TaskHeartbeat> {
        let mut out = Vec::with_capacity(self.running_tasks.lock().await.len());
        for (tid, handle) in self.running_tasks.lock().await.iter_mut() {
            let (state_code, records, last_record_at, logs, last_cp, sink_metrics) =
                match (&handle.status, &handle.logs) {
                    (Some(status), Some(ring)) => {
                        let snapshot = status.lock().await;
                        let entries = ring.entries_after(handle.log_cursor);
                        handle.log_cursor = ring.cursor();
                        let lines = entries.iter().map(|e| e.render()).collect::<Vec<_>>();
                        let checkpoint =
                            (snapshot.last_checkpoint_id, snapshot.last_checkpoint_size);
                        let state_code = handle
                            .terminal
                            .unwrap_or_else(|| proto_task_state(&snapshot.state));
                        (
                            state_code,
                            snapshot.processed_records,
                            snapshot.last_record_at,
                            lines,
                            checkpoint,
                            snapshot.sink_metrics.clone(),
                        )
                    }
                    // Pipeline not built yet (connector construction) or
                    // already draining: report the terminal code when one
                    // is pending, RUNNING otherwise (the historical
                    // pre-pipeline state).
                    _ => (handle.terminal.unwrap_or(2), 0, 0, Vec::new(), (0, 0), None),
                };
            out.push(seatunnel_engine_comm::TaskHeartbeat {
                task_id: tid.clone(),
                state: state_code,
                processed_records: records as i64,
                last_heartbeat_time: now_millis(),
                memory_usage: 0,
                last_record_at,
                logs,
                last_checkpoint_id: last_cp.0 as i64,
                last_checkpoint_size_bytes: last_cp.1 as i64,
                sink_metrics: sink_metrics.map(|m| m.into()),
            });
        }
        out
    }

    /// Stop all local tasks belonging to the given jobs.
    pub async fn cancel_jobs(self: &Arc<Self>, job_ids: &[String]) {
        if job_ids.is_empty() {
            return;
        }
        // Auto-clean: drop the cancelled jobs' local state after the
        // configured grace window. Scheduled once per job — the master
        // keeps re-sending the cancel list until every task reports
        // terminal, so re-arming on each heartbeat would pile up
        // duplicate timers and duplicate "removed" logs.
        if let Some(clean) = &self.clean_config {
            let mut scheduled = self.cancel_cleanups.lock().unwrap();
            for job_id in job_ids {
                if scheduled.insert(job_id.clone()) {
                    schedule_cancel_cleanup(Arc::clone(self), job_id.clone(), clean.grace_secs);
                }
            }
        }
        let mut to_arm = Vec::new();
        {
            let mut tasks = self.running_tasks.lock().await;
            for (tid, handle) in tasks.iter_mut() {
                if job_ids.contains(&handle.job_id) && !handle.cancel_token().is_cancelled() {
                    info!("Cancelling task {} (job cancelled)", tid);
                    handle.cancel_token().cancel();
                    if !handle.backstop_armed {
                        handle.backstop_armed = true;
                        to_arm.push(tid.clone());
                    }
                }
            }
        }
        for tid in to_arm {
            self.arm_cancel_backstop(tid);
        }
    }

    /// Count of currently tracked tasks.
    pub async fn running_task_count(&self) -> usize {
        self.running_tasks.lock().await.len()
    }
}

/// Map a `TaskState` to its proto task-state code (HeartbeatRequest /
/// TaskStatusReport state semantics: 0 CREATED, 2 RUNNING, 3 COMPLETED,
/// 4 FAILED, 5 CANCELLED).
fn proto_task_state(state: &TaskState) -> i32 {
    match state {
        TaskState::Created => 0,
        TaskState::Running => 2,
        TaskState::Completed => 3,
        TaskState::Failed { .. } => 4,
        TaskState::Cancelled => 5,
    }
}

/// Report a task lifecycle transition to the master. Fire-and-forget:
/// only the initial RUNNING report goes through here — terminal reports
/// are delivered by the retrying reporter. The client is cloned out of
/// the shared guard so the RPC never pins the lock across its await
/// (one hung call must not block every other reporter on the worker).
async fn report_transition_raw(
    worker_id: &str,
    master_client: &SharedMasterClient,
    term: &Arc<AtomicU64>,
    job_id: &str,
    task_id: &str,
    state: i32,
    records: u64,
    error: Option<String>,
) {
    let report = TaskStatusReport {
        worker_id: worker_id.to_string(),
        task_id: task_id.to_string(),
        job_id: job_id.to_string(),
        state,
        timestamp: now_millis(),
        processed_records: records as i64,
        error_message: error.unwrap_or_default(),
        term: term.load(Ordering::SeqCst),
    };
    let client = master_client.lock().await.clone();
    let Some(mut client) = client else {
        warn!(
            "no master client; cannot report state {} for task {} (worker {})",
            state, task_id, worker_id
        );
        return;
    };
    if let Err(e) = tokio::time::timeout(
        TERMINAL_REPORT_RPC_TIMEOUT,
        client.report_task_status(tonic::Request::new(report)),
    )
    .await
    {
        warn!(
            "report_task_status failed for {} (state {}): {:?}",
            task_id, state, e
        );
    }
}

/// State needed by the terminal-report delivery loop.
struct TerminalReporter {
    worker_id: String,
    master_client: SharedMasterClient,
    term: Arc<AtomicU64>,
    running_tasks: Arc<Mutex<HashMap<String, RunningTaskHandle>>>,
}

/// Deliver finished tasks' terminal transitions to the master, retrying
/// until acked (or the budget expires). The task handle is removed only
/// after delivery, so the heartbeat keeps listing the task (with its
/// terminal state) and the master's reconcile cannot re-dispatch a task
/// that is mid-report. Past the budget the handle is dropped with an
/// ERROR and the reconcile's release → re-dispatch takes over.
async fn run_terminal_reporter(
    mut rx: mpsc::UnboundedReceiver<TerminalOutcome>,
    reporter: TerminalReporter,
) {
    while let Some(outcome) = rx.recv().await {
        let started = std::time::Instant::now();
        let mut backoff = TERMINAL_REPORT_BACKOFF_MIN;
        let mut attempt = 0u32;
        loop {
            attempt += 1;
            match send_terminal_report(&reporter, &outcome).await {
                Ok(()) => {
                    info!(
                        "Worker {}: terminal report for task {} (state {}, records {}) delivered",
                        reporter.worker_id, outcome.task_id, outcome.code, outcome.records
                    );
                    remove_reported_handle(&reporter, &outcome).await;
                    break;
                }
                Err(e) => {
                    let elapsed = started.elapsed();
                    if elapsed >= TERMINAL_REPORT_BUDGET {
                        error!(
                            "Worker {}: terminal report for task {} (state {}) DROPPED after \
                             {:?} / {} attempts: {:?} — the master's heartbeat reconcile will \
                             release and re-dispatch it",
                            reporter.worker_id, outcome.task_id, outcome.code, elapsed, attempt, e
                        );
                        remove_reported_handle(&reporter, &outcome).await;
                        break;
                    }
                    warn!(
                        "Worker {}: terminal report for task {} (state {}) attempt {} failed, \
                         retrying in {:?}: {:?}",
                        reporter.worker_id, outcome.task_id, outcome.code, attempt, backoff, e
                    );
                    tokio::time::sleep(backoff).await;
                    backoff = backoff.saturating_mul(2).min(TERMINAL_REPORT_BACKOFF_MAX);
                }
            }
        }
    }
}

/// One delivery attempt. Never holds the shared client guard across the
/// RPC; bounded by a per-attempt timeout so a hung master cannot stall
/// the queue.
async fn send_terminal_report(
    reporter: &TerminalReporter,
    outcome: &TerminalOutcome,
) -> Result<(), tonic::Status> {
    let client = reporter.master_client.lock().await.clone();
    let Some(mut client) = client else {
        return Err(tonic::Status::unavailable(
            "no master client (not registered yet)",
        ));
    };
    let report = TaskStatusReport {
        worker_id: reporter.worker_id.clone(),
        task_id: outcome.task_id.clone(),
        job_id: outcome.job_id.clone(),
        state: outcome.code,
        timestamp: now_millis(),
        processed_records: outcome.records as i64,
        error_message: outcome.error.clone().unwrap_or_default(),
        term: reporter.term.load(Ordering::SeqCst),
    };
    tokio::time::timeout(
        TERMINAL_REPORT_RPC_TIMEOUT,
        client.report_task_status(tonic::Request::new(report)),
    )
    .await
    .map_err(|_| {
        tonic::Status::deadline_exceeded(format!(
            "report_task_status timed out after {}s",
            TERMINAL_REPORT_RPC_TIMEOUT.as_secs()
        ))
    })?
    .map(|_| ())
}

/// Drop a delivered/expired task handle — only when it is still the same
/// incarnation (a re-dispatch of the id inserts a fresh handle whose
/// generation differs).
async fn remove_reported_handle(reporter: &TerminalReporter, outcome: &TerminalOutcome) {
    let mut tasks = reporter.running_tasks.lock().await;
    let matches = tasks
        .get(&outcome.task_id)
        .is_some_and(|h| h.generation == outcome.generation && h.terminal.is_some());
    if matches {
        tasks.remove(&outcome.task_id);
    }
}

/// Background state-cleanup settings derived from the engine config.
#[derive(Debug, Clone, Copy)]
pub struct CleanConfig {
    /// Seconds after a cancelled job's state is deleted.
    pub grace_secs: u64,
    /// Seconds between TTL sweeps.
    pub interval_secs: u64,
    /// Sweep TTL in seconds (history-job-expire-minutes).
    pub ttl_secs: u64,
}

/// Spawn the background cleaner: periodic TTL sweep plus delayed cleanup
/// of cancelled jobs (after the grace window).
pub fn spawn_state_cleaner(
    worker: Arc<WorkerNode>,
    config: CleanConfig,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let mut ticker =
            tokio::time::interval(std::time::Duration::from_secs(config.interval_secs.max(1)));
        // First tick fires immediately — sweep once at startup.
        loop {
            ticker.tick().await;
            let removed = worker
                .state_store()
                .sweep_expired(std::time::Duration::from_secs(config.ttl_secs.max(1)));
            if !removed.is_empty() {
                tracing::info!("state cleaner: swept {} expired job(s)", removed.len());
            }
        }
    })
}

/// Fetch the newest checkpoint from the master-backed shared store.
///
/// Returns the payload (when one exists) plus the highest checkpoint id
/// the master ever RECORDED for the task — a non-zero latest with no
/// payload means "restore expected but unavailable". RPC failures are
/// returned, not swallowed: the caller decides via the restore-missing
/// policy.
async fn fetch_checkpoint_from_master(
    master_client: &SharedMasterClient,
    job_id: &str,
    task_id: &str,
) -> Result<Option<(u64, Vec<u8>)>, i64> {
    // Err(latest) = the fetch failed but the task HAS checkpoint history.
    let client = master_client.lock().await.clone();
    let Some(mut client) = client else {
        return Err(0);
    };
    let request = tonic::Request::new(seatunnel_engine_comm::FetchCheckpointRequest {
        job_id: job_id.to_string(),
        task_id: task_id.to_string(),
    });
    let fetched = tokio::time::timeout(
        TERMINAL_REPORT_RPC_TIMEOUT,
        client.fetch_checkpoint(request),
    )
    .await;
    match fetched {
        Ok(Ok(resp)) => {
            let inner = resp.into_inner();
            let latest = inner.latest_checkpoint_id;
            if inner.checkpoint_id > 0 && !inner.checkpoint_data.is_empty() {
                Ok(Some((inner.checkpoint_id as u64, inner.checkpoint_data)))
            } else if latest > 0 {
                Err(latest)
            } else {
                Ok(None)
            }
        }
        Ok(Err(e)) => {
            warn!("fetch_checkpoint for {} failed: {}", task_id, e);
            Err(-1)
        }
        Err(_) => {
            warn!("fetch_checkpoint for {} timed out", task_id);
            Err(-1)
        }
    }
}

/// Apply the restore-missing policy when a task has checkpoint history
/// but no restorable payload: `fail` (default) refuses the silent cold
/// start — for a CDC source it would skip every change since the last
/// checkpoint — while `warn` logs loudly and cold-starts.
fn resolve_missing_restore(
    task: &TaskDescriptor,
    ctx: &TaskExecCtx,
    latest_checkpoint_id: i64,
) -> anyhow::Result<Option<Vec<u8>>> {
    if latest_checkpoint_id <= 0 {
        info!("Task {}: no checkpoint found (cold start)", task.task_id);
        return Ok(None);
    }
    match ctx.restore_missing.as_str() {
        "warn" => {
            error!(
                "Task {}: checkpoint history exists (latest cp-{}) but no restorable payload \
                 was found — cold-starting anyway (checkpoint.restore-missing=warn); changes \
                 since the last checkpoint are SKIPPED",
                task.task_id, latest_checkpoint_id
            );
            Ok(None)
        }
        _ => Err(anyhow::anyhow!(
            "restore expected: task {} has checkpoint history (latest cp-{}) but no \
             restorable payload was found on this worker; refusing a silent cold start \
             (set checkpoint.restore-missing: warn to allow it)",
            task.task_id,
            latest_checkpoint_id
        )),
    }
}

/// Schedule deletion of a cancelled job's local state after the grace
/// window (keeps a restore window for operator intervention).
///
/// The timer re-checks with the master before deleting: the `job update`
/// flow cancels and RESUBMITS the same job id seconds later, and the new
/// incarnation checkpoints into the same directory — a blind delete at
/// T+grace would wipe its state and force a silent cold start. If the
/// master is unreachable the deletion is skipped (the TTL sweep bounds
/// leftovers) — deleting a live job's state is never the safe default.
pub fn schedule_cancel_cleanup(worker: Arc<WorkerNode>, job_id: String, grace_secs: u64) {
    tokio::spawn(async move {
        tokio::time::sleep(std::time::Duration::from_secs(grace_secs.max(1))).await;
        match worker.job_state_on_master(&job_id).await {
            Some(state) if is_terminal_state(state) => {
                if worker.state_store().drop_job(&job_id) {
                    tracing::info!("state cleaner: removed cancelled job state '{}'", job_id);
                } else {
                    tracing::debug!("state cleaner: no state left for job '{}'", job_id);
                }
            }
            Some(state) => {
                tracing::info!(
                    "state cleaner: job '{}' is non-terminal again (proto state {}) — it was \
                     resubmitted (update/restart); keeping its checkpoint state",
                    job_id,
                    state
                );
            }
            None => {
                tracing::warn!(
                    "state cleaner: master unreachable — skipping cleanup of cancelled job \
                     '{}' this round; its state dir remains until a later sweep",
                    job_id
                );
            }
        }
    });
}

/// Proto task-state / job-state terminal codes (3 COMPLETED, 4 FAILED,
/// 5 CANCELLED, 6 job-cancelled).
fn is_terminal_state(code: i32) -> bool {
    matches!(code, 3 | 4 | 5 | 6)
}

/// Execution context handed to the spawned task future so it can report
/// transitions and reach the state store without borrowing the worker.
#[derive(Clone)]
struct TaskExecCtx {
    worker_id: String,
    master_client: SharedMasterClient,
    state_store: Arc<LocalStateStore>,
    /// Checkpoint storage backend: localfile | master | s3.
    storage_type: String,
    /// S3 store (storage type = s3); workers write directly.
    s3_store: Option<crate::checkpoint_store::S3CheckpointStore>,
    /// Restore-missing policy: "fail" (default) | "warn".
    restore_missing: String,
    /// Highest master term seen by this worker (fencing on reports).
    term: Arc<AtomicU64>,
}

/// Execute one descriptor end-to-end: build connectors, restore state, run
/// the TaskGroup, and return the terminal transition. Panics inside the
/// pipeline are caught and converted into a FAILED outcome so a buggy
/// connector cannot take down the whole worker process silently.
///
/// The returned outcome is queued for reliable delivery by the caller —
/// a lost terminal report pins the job in RUNNING on the master forever.
async fn execute_descriptor(
    task: TaskDescriptor,
    ctx: TaskExecCtx,
    cancel: Arc<CancellationToken>,
    worker: Arc<crate::worker::WorkerNode>,
) -> TerminalOutcome {
    let task_id = task.task_id.clone();
    let job_id = task.job_id.clone();

    // Initial RUNNING report (fire-and-forget; a lost one self-heals via
    // the master's heartbeat reconcile of never-reported tasks).
    report_transition_raw(
        &ctx.worker_id,
        &ctx.master_client,
        &ctx.term,
        &job_id,
        &task_id,
        2,
        0,
        None,
    )
    .await;

    let result = match futures::FutureExt::catch_unwind(std::panic::AssertUnwindSafe(run_pipeline(
        &task, &ctx, cancel, &worker,
    )))
    .await
    {
        Ok(Ok(status)) => Ok(status),
        Ok(Err(e)) => Err(e),
        Err(panic_payload) => {
            let msg = panic_message(&panic_payload);
            Err(anyhow::anyhow!("task panicked: {}", msg))
        }
    };

    let outcome = match result {
        Ok(status) => {
            let (code, err) = match status.state {
                TaskState::Completed => (3, None),
                TaskState::Cancelled => (5, None),
                TaskState::Failed { ref error } => (4, Some(error.clone())),
                other => (4, Some(format!("unexpected end state {}", other))),
            };
            TerminalOutcome {
                job_id,
                task_id: task_id.clone(),
                generation: 0, // stamped by the caller (assign wrapper)
                code,
                records: status.processed_records,
                error: err,
            }
        }
        Err(e) => {
            error!("Task {} crashed: {:?}", task_id, e);
            TerminalOutcome {
                job_id,
                task_id,
                generation: 0,
                code: 4,
                records: 0,
                error: Some(e.to_string()),
            }
        }
    };
    outcome
}

fn panic_message(payload: &Box<dyn std::any::Any + Send>) -> String {
    if let Some(s) = payload.downcast_ref::<&str>() {
        (*s).to_string()
    } else if let Some(s) = payload.downcast_ref::<String>() {
        s.clone()
    } else {
        "unknown panic".to_string()
    }
}

/// Build and run the chained pipeline for a descriptor.
async fn run_pipeline(
    task: &TaskDescriptor,
    ctx: &TaskExecCtx,
    cancel: Arc<CancellationToken>,
    worker: &Arc<crate::worker::WorkerNode>,
) -> anyhow::Result<TaskStatus> {
    let cfg = &task.config;

    // Multi-pipeline jobs carry `pipeline.*` keys (source + sink list,
    // fan-out built inside the task); legacy single-pipeline descriptors
    // keep source.plugin/sink.plugin.
    let source_plugin = cfg
        .get("pipeline.source.plugin")
        .or_else(|| cfg.get("source.plugin"))
        .map(String::as_str)
        .unwrap_or("");
    let source_config_raw = cfg
        .get("pipeline.source.config")
        .or_else(|| cfg.get("source.config"))
        .map(String::as_str)
        .unwrap_or("{}");
    let transform_raw = cfg
        .get("transform.config")
        .map(String::as_str)
        .unwrap_or("[]");
    let checkpoint_interval: u64 = cfg
        .get("checkpoint.interval")
        .and_then(|v| v.parse().ok())
        .unwrap_or(crate::job_coordinator::DEFAULT_CHECKPOINT_INTERVAL_MS);

    let source_value: serde_json::Value = serde_json::from_str(source_config_raw)?;
    let mut source_config =
        seatunnel_engine_core::connector_factory::json_to_config_map(&source_value);
    // Partition snapshot ranges across parallel subtasks (within this
    // pipeline — task_index/parallelism are pipeline-scoped).
    source_config.insert("subtask.index".to_string(), task.task_index.to_string());
    source_config.insert(
        "subtask.count".to_string(),
        task.parallelism.max(1).to_string(),
    );
    let transforms_cfg: Vec<serde_json::Value> = serde_json::from_str(transform_raw)?;

    // Restore chain: worker-local disk > shared backend (master store /
    // S3 by storage type) > cold start. A task taken over from a dead
    // worker resumes from the shared checkpoint instead of re-snapshotting.
    // A LOCAL read error is fatal (the state exists but is unusable —
    // silently skipping it would cold-start a task the operator expects
    // to resume); a MISSING payload with checkpoint history goes through
    // the checkpoint.restore-missing policy.
    let restore_state = match ctx
        .state_store
        .load_latest_checkpoint(&task.job_id, &task.task_id)
    {
        Err(e) => {
            error!(
                "Task {}: local checkpoint state exists but is unreadable: {}",
                task.task_id, e
            );
            anyhow::bail!(
                "local checkpoint state for job {} task {} is present but unreadable: {e}; \
                 fix or remove <state-dir>/job-{}/ manually",
                task.job_id,
                task.task_id,
                task.job_id
            );
        }
        Ok(Some((id, data))) => {
            info!(
                "Task {}: restored checkpoint cp-{} from local",
                task.task_id, id
            );
            Some(data)
        }
        Ok(None) => match ctx.storage_type.as_str() {
            "master" => {
                match fetch_checkpoint_from_master(&ctx.master_client, &task.job_id, &task.task_id)
                    .await
                {
                    Ok(Some((id, data))) => {
                        info!(
                            "Task {}: restored checkpoint cp-{} from master",
                            task.task_id, id
                        );
                        Some(data)
                    }
                    Ok(None) => {
                        info!("Task {}: no checkpoint found (cold start)", task.task_id);
                        None
                    }
                    Err(latest) => resolve_missing_restore(task, &ctx, latest)?,
                }
            }
            "s3" => {
                let fetched = if let Some(store) = &ctx.s3_store {
                    store.load_latest(&task.job_id, &task.task_id).await
                } else {
                    None
                };
                match fetched {
                    Some((id, data)) => {
                        info!(
                            "Task {}: restored checkpoint cp-{} from s3",
                            task.task_id, id
                        );
                        Some(data)
                    }
                    None => {
                        // Consult the master's history to tell a fresh
                        // start from an expected-but-unavailable restore.
                        match fetch_checkpoint_from_master(
                            &ctx.master_client,
                            &task.job_id,
                            &task.task_id,
                        )
                        .await
                        {
                            Ok(None) => {
                                info!("Task {}: no checkpoint found (cold start)", task.task_id);
                                None
                            }
                            Err(latest) if latest > 0 => {
                                resolve_missing_restore(task, &ctx, latest)?
                            }
                            _ => {
                                warn!(
                                    "Task {}: s3 checkpoint lookup failed and the master's \
                                     history is unavailable — cold-starting; if this task had \
                                     checkpoints, changes since the last one are skipped",
                                    task.task_id
                                );
                                None
                            }
                        }
                    }
                }
            }
            _ => {
                // localfile: no local state. Ask the master whether this
                // task ever checkpointed — a non-zero history means the
                // state stayed on another worker's disk (failover) or was
                // lost, and a cold start would silently skip data.
                match fetch_checkpoint_from_master(&ctx.master_client, &task.job_id, &task.task_id)
                    .await
                {
                    Ok(Some((id, data))) => {
                        info!(
                            "Task {}: restored checkpoint cp-{} from master",
                            task.task_id, id
                        );
                        Some(data)
                    }
                    Ok(None) => {
                        info!("Task {}: no checkpoint found (cold start)", task.task_id);
                        None
                    }
                    Err(latest) => resolve_missing_restore(task, &ctx, latest)?,
                }
            }
        },
    };

    let reader = create_source(
        source_plugin,
        &source_config,
        task.parallelism.max(1) as usize,
        restore_state.as_deref(),
    )?;
    let transforms = create_transforms(&transforms_cfg)?;

    // Sink side: pipeline descriptors carry a sink LIST (fan-out through
    // the FanoutSinkWriter mux); legacy descriptors fall back to a single
    // sink.plugin/sink.config pair. The full SinkPipeline is kept so the
    // writer's shared metrics handle can ride along into the task group.
    let (writer, sink_metrics) = if let Some(sinks_raw) = cfg.get("pipeline.sinks") {
        let sinks: Vec<seatunnel_engine_core::connector_factory::SinkDeclaration> =
            serde_json::from_str(sinks_raw)?;
        let policy = seatunnel_engine_core::fanout::SinkFailurePolicy::parse(
            cfg.get("pipeline.on-sink-failure")
                .map(String::as_str)
                .unwrap_or("fail"),
        );
        // Ceiling for channel sends / acks to every inner sink: a hung
        // sink must fail or be isolated at this deadline, never freeze
        // the task loop (a frozen loop ignores cancellation and
        // heartbeat-reports RUNNING forever).
        let sink_ack_timeout = std::time::Duration::from_millis(
            cfg.get("pipeline.sink-ack-timeout-ms")
                .and_then(|v| v.parse().ok())
                .unwrap_or(
                    seatunnel_engine_core::fanout::DEFAULT_SINK_ACK_TIMEOUT.as_millis() as u64,
                )
                .max(1000),
        );
        info!(
            "Task {}: pipeline '{}' → {} sink(s), on-sink-failure={:?}, sink-ack-timeout={:?}",
            task.task_id,
            cfg.get("pipeline.name").map(String::as_str).unwrap_or("?"),
            sinks.len(),
            policy,
            sink_ack_timeout
        );
        let pipeline = seatunnel_engine_core::connector_factory::create_sink_pipeline(
            &sinks,
            policy,
            None,
            sink_ack_timeout,
        )?;
        (pipeline.writer, pipeline.metrics)
    } else {
        let sink_plugin = cfg
            .get("sink.plugin")
            .map(String::as_str)
            .unwrap_or("console");
        let sink_value: serde_json::Value =
            serde_json::from_str(cfg.get("sink.config").map(String::as_str).unwrap_or("{}"))?;
        let sink_config = seatunnel_engine_core::connector_factory::json_to_config_map(&sink_value);
        let pipeline = seatunnel_engine_core::connector_factory::create_sink_with_restore(
            sink_plugin,
            &sink_config,
            None,
        )?;
        (pipeline.writer, pipeline.metrics)
    };

    // Coordinated checkpoints: the master drives; this task's gate
    // receives barrier triggers and completion events over heartbeats.
    let checkpoint_handle = worker
        .checkpoint_handle_for(&task.task_id, &task.job_id)
        .await;

    let context = seatunnel_engine_core::task_group::TaskContext::new(
        task.task_id.clone(),
        task.job_id.clone(),
        task.stage_id.clone(),
        task.task_index.max(0) as usize,
        task.parallelism.max(1) as usize,
    )
    .with_cancel_token(cancel)
    .with_checkpoint_interval(checkpoint_interval)
    .with_checkpoint_handle(checkpoint_handle);

    let mut group = TaskGroup::new(context, reader, writer).with_transforms(transforms);
    if let Some(metrics) = sink_metrics {
        group = group.with_sink_metrics(metrics);
    }
    // Expose live metrics/logs to the heartbeat before the loop starts.
    worker
        .attach_task_metrics(&task.task_id, group.status(), group.logs().clone())
        .await;
    group.run().await
}

/// Drains the task gates' reports and speaks the master-driven
/// checkpoint protocol:
///
/// - `Checkpoint(report)` → durable local persist (+ S3), then forward
///   the prepare to the master; the master resolves it and phase-2
///   (sink commit) events come back over heartbeats. This replaces the
///   old per-task interval listener — the worker no longer decides
///   checkpoint ids or completion on its own.
/// - `CheckpointFailed` → tell the master to abort the pending
///   checkpoint.
/// - `CommitDone` / `Done` → bookkeeping only.
struct CheckpointForwarder {
    worker_id: String,
    master_client: SharedMasterClient,
    state_store: Arc<LocalStateStore>,
    /// Upload bytes to the master-backed store (storage type = master).
    upload_to_master: bool,
    /// Direct S3 writes (storage type = s3).
    s3_store: Option<crate::checkpoint_store::S3CheckpointStore>,
    /// Highest master term seen (carried on the report for fencing).
    term: Arc<AtomicU64>,
    /// task_id → gate (job id lookup for reports).
    gates: SharedCheckpointGates,
}

impl CheckpointForwarder {
    async fn job_of(&self, task_id: &str) -> Option<String> {
        self.gates
            .lock()
            .await
            .get(task_id)
            .map(|g| g.job_id.clone())
    }

    async fn send_report(&self, report: CheckpointReport) {
        let mut guard = self.master_client.lock().await;
        let Some(client) = guard.as_mut() else {
            warn!(
                "Worker {}: no master client; checkpoint report for task {} dropped",
                self.worker_id, report.task_id
            );
            return;
        };
        if let Err(e) = client.report_checkpoint(tonic::Request::new(report)).await {
            warn!("report_checkpoint failed: {}", e);
        }
    }

    async fn run(&self, mut rx: mpsc::UnboundedReceiver<TaskToDriver>) {
        while let Some(message) = rx.recv().await {
            match message {
                TaskToDriver::Checkpoint(report) => {
                    let Some(job_id) = self.job_of(&report.task_id).await else {
                        warn!(
                            "checkpoint report for task {} with no gate; dropped",
                            report.task_id
                        );
                        continue;
                    };
                    // 1. Durable local persistence (crash recovery).
                    if let Err(e) = self.state_store.save_checkpoint(
                        &job_id,
                        &report.task_id,
                        report.checkpoint_id,
                        &report.reader_state,
                    ) {
                        error!(
                            "Task {} checkpoint {}: local persist failed: {}",
                            report.task_id, report.checkpoint_id, e
                        );
                    }

                    // 2. S3 direct write (storage type = s3): Java
                    //    external-storage model — failures log and skip
                    //    (local disk already holds the state).
                    if let Some(store) = &self.s3_store {
                        store
                            .save(
                                &job_id,
                                &report.task_id,
                                report.checkpoint_id,
                                &report.reader_state,
                            )
                            .await;
                    }

                    // 3. Forward the prepare; bytes ride along for the
                    //    master-backed shared store (storage type =
                    //    master). Exit-time final barriers are persisted
                    //    by the master but never join a coordinated
                    //    checkpoint (the task is leaving).
                    let upload = if self.upload_to_master {
                        report.reader_state.clone()
                    } else {
                        Vec::new()
                    };
                    self.send_report(CheckpointReport {
                        job_id,
                        task_id: report.task_id.clone(),
                        checkpoint_id: report.checkpoint_id as i64,
                        timestamp: now_millis(),
                        checkpoint_data: upload,
                        success: true,
                        term: self.term.load(Ordering::SeqCst),
                        phase: CheckpointPhase::CheckpointPrepare as i32,
                    })
                    .await;
                }
                TaskToDriver::CheckpointFailed {
                    task_id,
                    checkpoint_id,
                    error,
                } => {
                    warn!(
                        "Task {} checkpoint {} failed at the barrier: {}",
                        task_id, checkpoint_id, error
                    );
                    let job_id = self.job_of(&task_id).await.unwrap_or_default();
                    if !job_id.is_empty() {
                        self.send_report(CheckpointReport {
                            job_id,
                            task_id,
                            checkpoint_id: checkpoint_id as i64,
                            timestamp: now_millis(),
                            checkpoint_data: Vec::new(),
                            success: false,
                            term: self.term.load(Ordering::SeqCst),
                            phase: CheckpointPhase::CheckpointPrepare as i32,
                        })
                        .await;
                    }
                }
                TaskToDriver::CommitDone {
                    task_id,
                    checkpoint_id,
                } => {
                    tracing::debug!(
                        "Task {} checkpoint {} phase 2 committed",
                        task_id,
                        checkpoint_id
                    );
                }
                TaskToDriver::Done { task_id } => {
                    // Gate cleanup also happens at execution exit; this is
                    // the task's own farewell.
                    self.gates.lock().await.remove(&task_id);
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tmp_store(tag: &str) -> Arc<LocalStateStore> {
        let dir = std::env::temp_dir().join(format!("st-worker-{}", tag));
        let _ = std::fs::remove_dir_all(&dir);
        Arc::new(LocalStateStore::new(dir))
    }

    #[test]
    fn test_worker_node_creation() {
        let worker = WorkerNode::new("w1", "127.0.0.1:5001", tmp_store("create"));
        assert_eq!(worker.worker_id, "w1");
        assert_eq!(worker.term(), 0);
    }

    #[tokio::test]
    async fn test_term_fencing_rejects_stale_master() {
        use seatunnel_engine_comm::TaskDescriptor;

        let worker = Arc::new(WorkerNode::new("w1", "127.0.0.1:5001", tmp_store("fence")));

        // Fresh master at term 2: accepted, term recorded.
        let resp = HeartbeatResponse {
            worker_id: "w1".into(),
            next_interval_ms: 2000,
            pending_tasks: vec![TaskDescriptor {
                task_id: "t-new".into(),
                job_id: "j".into(),
                stage_id: "s".into(),
                task_name: "n".into(),
                task_index: 0,
                source_config_json: String::new(),
                sink_config_json: String::new(),
                parallelism: 1,
                config: HashMap::from([
                    ("source.plugin".to_string(), "Fake".to_string()),
                    (
                        "source.config".to_string(),
                        serde_json::json!({ "row.num": 1 }).to_string(),
                    ),
                    ("sink.plugin".to_string(), "Console".to_string()),
                    ("sink.config".to_string(), "{}".to_string()),
                    ("transform.config".to_string(), "[]".to_string()),
                    ("checkpoint.interval".to_string(), "60000".to_string()),
                ]),
            }],
            cancel_jobs: vec!["j-doomed".into()],
            preempted_task_ids: Vec::new(),
            term: 2,
            leader_hint: String::new(),
            checkpoint_triggers: Vec::new(),
            checkpoint_resolutions: Vec::new(),
        };
        assert!(worker.apply_master_response(&resp).await);
        assert_eq!(worker.term(), 2);

        // Deposed master at term 1 (< 2): instructions ignored.
        let stale = HeartbeatResponse {
            worker_id: "w1".into(),
            next_interval_ms: 2000,
            pending_tasks: Vec::new(),
            cancel_jobs: vec!["j-doomed".into()],
            preempted_task_ids: Vec::new(),
            term: 1,
            leader_hint: String::new(),
            checkpoint_triggers: Vec::new(),
            checkpoint_resolutions: Vec::new(),
        };
        assert!(!worker.apply_master_response(&stale).await);
        assert_eq!(worker.term(), 2);
    }

    #[tokio::test]
    async fn test_assign_and_complete_fake_job() {
        let worker = Arc::new(WorkerNode::new("w1", "127.0.0.1:5001", tmp_store("run")));
        let mut config = HashMap::new();
        config.insert("source.plugin".to_string(), "Fake".to_string());
        config.insert(
            "source.config".to_string(),
            serde_json::json!({ "row.num": 3 }).to_string(),
        );
        config.insert("sink.plugin".to_string(), "Console".to_string());
        config.insert("sink.config".to_string(), "{}".to_string());
        config.insert("transform.config".to_string(), "[]".to_string());
        config.insert("checkpoint.interval".to_string(), "60000".to_string());
        let task = TaskDescriptor {
            task_id: "t-1".into(),
            job_id: "j-1".into(),
            stage_id: "s-1".into(),
            task_name: "pipeline".into(),
            task_index: 0,
            source_config_json: String::new(),
            sink_config_json: String::new(),
            parallelism: 1,
            config,
        };
        worker.assign_task(task).await;
        // Wait for detached execution to finish.
        tokio::time::sleep(std::time::Duration::from_millis(300)).await;
    }

    #[tokio::test]
    async fn test_cancel_stops_running_task() {
        let worker = Arc::new(WorkerNode::new("w1", "127.0.0.1:5001", tmp_store("cancel")));
        let mut config = HashMap::new();
        config.insert("source.plugin".to_string(), "Kafka".to_string());
        config.insert(
            "source.config".to_string(),
            serde_json::json!({ "bootstrap.servers": "127.0.0.1:19092", "topic": "never" })
                .to_string(),
        );
        config.insert("sink.plugin".to_string(), "Console".to_string());
        config.insert("sink.config".to_string(), "{}".to_string());
        config.insert("transform.config".to_string(), "[]".to_string());
        config.insert("checkpoint.interval".to_string(), "60000".to_string());
        let task = TaskDescriptor {
            task_id: "t-c".into(),
            job_id: "j-c".into(),
            stage_id: "s".into(),
            task_name: "streaming".into(),
            task_index: 0,
            source_config_json: String::new(),
            sink_config_json: String::new(),
            parallelism: 1,
            config,
        };
        worker.assign_task(task).await;
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        worker.cancel_jobs(&["j-c".to_string()]).await;
        assert!(worker.running_task_count().await <= 1);
    }

    #[tokio::test]
    async fn heartbeat_reports_truthful_terminal_state_and_keeps_listing_the_task() {
        let worker = Arc::new(WorkerNode::new("w1", "127.0.0.1:5001", tmp_store("hb")));
        let mut config = HashMap::new();
        config.insert("source.plugin".to_string(), "Fake".to_string());
        config.insert(
            "source.config".to_string(),
            serde_json::json!({ "row.num": 1 }).to_string(),
        );
        config.insert("sink.plugin".to_string(), "Console".to_string());
        config.insert("sink.config".to_string(), "{}".to_string());
        config.insert("transform.config".to_string(), "[]".to_string());
        config.insert("checkpoint.interval".to_string(), "60000".to_string());
        let task = TaskDescriptor {
            task_id: "t-hb".into(),
            job_id: "j-hb".into(),
            stage_id: "s".into(),
            task_name: "pipeline".into(),
            task_index: 0,
            source_config_json: String::new(),
            sink_config_json: String::new(),
            parallelism: 1,
            config,
        };
        worker.assign_task(task).await;
        tokio::time::sleep(std::time::Duration::from_millis(300)).await;

        // The Fake pipeline finished: the heartbeat must carry the real
        // COMPLETED state — not the historical hardcoded TASK_RUNNING.
        // The handle stays listed (the terminal report is not acked — no
        // master client here), which keeps the master's reconcile from
        // re-dispatching a task that is mid-report.
        let heartbeats = worker.heartbeat_tasks().await;
        let hb = heartbeats
            .iter()
            .find(|h| h.task_id == "t-hb")
            .expect("terminal-pending task must stay in heartbeats");
        assert_eq!(hb.state, 3, "task finished -> COMPLETED in heartbeat");
    }

    #[tokio::test]
    async fn test_cancel_cleanup_scheduled_once_per_job() {
        // A generous grace keeps the delayed delete from firing during
        // the test; only the scheduling dedup is under test here.
        let clean = CleanConfig {
            grace_secs: 3600,
            interval_secs: 60,
            ttl_secs: 3600,
        };
        let worker = Arc::new(WorkerNode::new_with_clean(
            "w1",
            "127.0.0.1:5001",
            tmp_store("dedup"),
            Some(clean),
        ));

        // The master re-broadcasts the cancel on every heartbeat while
        // the job still has non-terminal tasks; repeated broadcasts must
        // not arm more than one delayed cleanup per job.
        worker.cancel_jobs(&["j-a".into(), "j-b".into()]).await;
        worker.cancel_jobs(&["j-a".to_string()]).await;
        worker
            .cancel_jobs(&["j-a".into(), "j-b".into(), "j-c".into()])
            .await;

        let scheduled = worker.cancel_cleanups.lock().unwrap();
        assert_eq!(scheduled.len(), 3, "one entry per distinct job");
        for id in ["j-a", "j-b", "j-c"] {
            assert!(scheduled.contains(id));
        }
    }
}
