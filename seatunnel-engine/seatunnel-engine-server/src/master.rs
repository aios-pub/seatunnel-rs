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

//! Master service handler: worker registry, task dispatch over heartbeat,
//! status/checkpoint ingestion.

use std::collections::HashMap;
use std::sync::Arc;

use seatunnel_engine_comm::{
    CheckpointReport, Empty, FetchCheckpointRequest, FetchCheckpointResponse, HeartbeatRequest,
    HeartbeatResponse, JobStatus, JobStatusRequest, MasterService, TaskStatusReport,
    UnregisterWorkerRequest, WorkerRegistration, WorkerRegistrationResponse,
};
use tokio::sync::Mutex;
use tonic::{Request, Response, Status};
use tracing::{info, warn};

use crate::job_coordinator::{Command, JobCoordinator, JobState, WorkerState};
use crate::raft::WritePath;

/// A registered worker's address, liveness and measured admission state.
#[derive(Debug, Clone)]
pub struct WorkerEntry {
    pub address: String,
    pub last_heartbeat_ms: i64,
    /// Measured pressure 0..1000 (per-mille) — placement orders by this.
    pub load_score: u32,
    /// Event-loop lag EMA (ms) as last reported.
    pub lag_ms: u32,
    /// RSS over usable memory (per-mille) as last reported.
    pub mem_permille: u32,
    /// Host CPU usage (per-mille) as last reported (display signal).
    pub cpu_permille: u32,
    /// False while the worker is over a pressure watermark: no new
    /// tasks; its PENDING tasks may be stolen by healthy peers.
    pub can_accept: bool,
}

impl WorkerEntry {
    /// Freshly-registered default: unknown signals, accepting.
    pub fn new(address: String) -> Self {
        WorkerEntry {
            address,
            last_heartbeat_ms: seatunnel_engine_core::now_millis(),
            load_score: 0,
            lag_ms: 0,
            mem_permille: 0,
            cpu_permille: 0,
            can_accept: true,
        }
    }

    fn state(&self) -> WorkerState {
        if self.can_accept {
            WorkerState::Healthy
        } else {
            WorkerState::Overloaded
        }
    }
}

/// Shared between MasterService (registration/heartbeats) and ClientService
/// (scheduling decisions) so submissions always see live workers.
pub type WorkerRegistry = Arc<std::sync::RwLock<HashMap<String, WorkerEntry>>>;

pub fn new_worker_registry() -> WorkerRegistry {
    Arc::new(std::sync::RwLock::new(HashMap::new()))
}

/// Snapshot of registered workers as `(id, address)` pairs.
pub fn registry_snapshot(registry: &WorkerRegistry) -> Vec<(String, String)> {
    registry
        .read()
        .unwrap()
        .iter()
        .map(|(id, e)| (id.clone(), e.address.clone()))
        .collect()
}

/// Snapshot of registered workers as `(id, address, load_score,
/// can_accept)` — the placement input for pressure-ordered scheduling.
pub fn registry_snapshot_admission(registry: &WorkerRegistry) -> Vec<(String, String, u32, bool)> {
    registry
        .read()
        .unwrap()
        .iter()
        .map(|(id, e)| (id.clone(), e.address.clone(), e.load_score, e.can_accept))
        .collect()
}

/// Identity of this master node, shared by Master/Client handlers so the
/// wire protocol can carry a real leader address and role.
#[derive(Debug, Clone)]
pub struct MasterInfo {
    /// Address other nodes should use to reach this master (advertise
    /// address, not the bind wildcard).
    pub advertise_addr: String,
    /// Deployment role: master | hybrid.
    pub role: String,
}

/// Master node state.
#[derive(Default)]
pub struct MasterState {
    /// Task assignments: task_id → worker_id.
    pub task_assignments: HashMap<String, String>,
}

#[derive(Debug, Clone)]
pub struct JobInfo {
    pub job_id: String,
    pub job_name: String,
    pub state: String,
    pub parallelism: i32,
    pub start_time: i64,
    pub end_time: Option<i64>,
    pub error_message: Option<String>,
}

/// Master service handler with real job coordination.
pub struct MasterHandler {
    state: Mutex<MasterState>,
    coordinator: Arc<JobCoordinator>,
    workers: WorkerRegistry,
    /// Durable mutations (Direct: in-place; Raft: consensus).
    writes: Arc<dyn WritePath>,
    /// Long-poll wake signal: any event that could give SOME worker new
    /// instructions (submission, cancel, fence, checkpoint resolution)
    /// fires this so parked heartbeats recompute immediately.
    wake: Arc<tokio::sync::Notify>,
    info: MasterInfo,
    /// Configured worker heartbeat period, echoed via `next_interval_ms`.
    heartbeat_interval_ms: u64,
    /// Soft liveness threshold: a worker silent longer than this gets no
    /// new assignments until it proves liveness again (still registered).
    worker_soft_timeout_ms: u64,
    /// Max tasks handed to one worker per heartbeat (rate fuse for the
    /// admission-signal blind window; 0 = unlimited). NOT a slot count.
    dispatch_batch_limit: u32,
    /// Cancel deadline: a cancelled job's tasks still non-terminal after
    /// this long are forced CANCELLED (stops the endless cancel
    /// re-broadcast when a task hangs or its terminal report is lost).
    cancel_force_timeout_ms: u64,
    /// First heartbeat at which each `Running` task (keyed by task id)
    /// was seen absent from its owner's report — restart-recovery grace
    /// tracking, see [`Self::reconcile_heartbeat_lost_tasks`].
    lost_since: std::sync::Mutex<HashMap<String, i64>>,
    /// How long a Running task may stay absent from its owner's
    /// heartbeats before it is declared lost work of a previous process
    /// lifetime and released for re-dispatch.
    lost_grace_ms: i64,
}

impl MasterHandler {
    pub fn new(
        coordinator: Arc<JobCoordinator>,
        workers: WorkerRegistry,
        info: MasterInfo,
        heartbeat_interval_ms: u64,
        worker_soft_timeout_ms: u64,
        writes: Arc<dyn WritePath>,
    ) -> Self {
        let heartbeat_interval_ms = heartbeat_interval_ms.clamp(250, 60_000);
        MasterHandler {
            state: Mutex::new(MasterState::default()),
            coordinator,
            workers,
            writes,
            wake: Arc::new(tokio::sync::Notify::new()),
            info,
            heartbeat_interval_ms,
            worker_soft_timeout_ms: worker_soft_timeout_ms.max(1_000),
            dispatch_batch_limit: 16,
            cancel_force_timeout_ms: 300_000,
            lost_since: std::sync::Mutex::new(HashMap::new()),
            // Six heartbeat periods, floor 30s: comfortably above any
            // dispatch latency (a freshly dispatched task misses at most
            // one heartbeat report), far below the zombie-forever
            // alternative when the registration-time reconcile raced the
            // Raft replay.
            lost_grace_ms: ((heartbeat_interval_ms * 6).max(30_000)) as i64,
        }
    }

    /// Override the per-heartbeat dispatch batch limit.
    pub fn with_dispatch_batch_limit(mut self, limit: u32) -> Self {
        self.dispatch_batch_limit = limit;
        self
    }

    /// Override the cancel deadline (see [`Self::cancel_force_timeout_ms`]).
    pub fn with_cancel_force_timeout(mut self, ms: u64) -> Self {
        self.cancel_force_timeout_ms = ms.max(30_000);
        self
    }

    /// The configured cancel deadline.
    pub fn cancel_force_timeout_ms(&self) -> u64 {
        self.cancel_force_timeout_ms
    }

    /// Heartbeat-time restart recovery: return the active (`Running` /
    /// `Deploying`) tasks the coordinator still pins on this worker but
    /// which have now been absent from its heartbeat reports for at
    /// least the grace window.
    ///
    /// Registration-time reconcile ([`Self::reattach_tasks`]) is the
    /// primary recovery path, but a hybrid node's embedded worker can
    /// register while the Raft log is still replaying — that reconcile
    /// then sees an empty coordinator, and once leadership installs the
    /// replayed tasks nothing re-runs it (the claim rule only releases
    /// tasks of `Dead` owners), leaving them Running forever with nobody
    /// executing them. The heartbeat task list is the truth of what this
    /// process actually runs, so a task missing from it longer than the
    /// grace window is lost work of a previous process lifetime. The
    /// grace window rides out the dispatch race where a task was handed
    /// out in the very heartbeat whose report lacks it.
    fn reconcile_heartbeat_lost_tasks(
        &self,
        worker_id: &str,
        reported: &[String],
        now_ms: i64,
    ) -> Vec<crate::job_coordinator::StuckCancelledTask> {
        let missing = self
            .coordinator
            .reconcile_lost_active_tasks(worker_id, reported);
        let mut seen = self.lost_since.lock().unwrap();
        let missing_ids: std::collections::HashSet<&str> =
            missing.iter().map(|m| m.task_id.as_str()).collect();
        seen.retain(|task_id, _| {
            // Running again (dispatch caught up) or no longer active on
            // this worker in the coordinator (released elsewhere /
            // terminal) — either way the tracking entry is obsolete.
            if reported.iter().any(|r| r == task_id) {
                return false;
            }
            missing_ids.contains(task_id.as_str())
        });
        let mut due = Vec::new();
        for lost in missing {
            let since = *seen.entry(lost.task_id.clone()).or_insert(now_ms);
            if now_ms.saturating_sub(since) >= self.lost_grace_ms {
                seen.remove(&lost.task_id);
                due.push(lost);
            }
        }
        due
    }

    /// Wake every parked long-poll heartbeat (new work may exist).
    pub fn wake_heartbeats(&self) {
        self.wake.notify_waiters();
    }

    /// Shared wake signal (for sibling handlers and background loops).
    pub fn wake_signal(&self) -> Arc<tokio::sync::Notify> {
        Arc::clone(&self.wake)
    }

    /// Convenience constructor for tests / embedded setups: direct
    /// in-process write path.
    pub fn new_direct(
        coordinator: Arc<JobCoordinator>,
        workers: WorkerRegistry,
        info: MasterInfo,
        heartbeat_interval_ms: u64,
        worker_soft_timeout_ms: u64,
    ) -> Self {
        Self::new(
            coordinator.clone(),
            workers,
            info,
            heartbeat_interval_ms,
            worker_soft_timeout_ms,
            Arc::new(crate::raft::DirectWrite::new(coordinator)),
        )
    }

    /// Classify a worker for claim decisions (registry view).
    fn classify(&self, worker_id: &str) -> WorkerState {
        match self.workers.read().unwrap().get(worker_id) {
            None => WorkerState::Dead,
            Some(entry) => entry.state(),
        }
    }

    pub fn coordinator(&self) -> &Arc<JobCoordinator> {
        &self.coordinator
    }

    pub fn worker_registry(&self) -> &WorkerRegistry {
        &self.workers
    }

    pub fn info(&self) -> &MasterInfo {
        &self.info
    }
}

impl MasterHandler {
    /// Classify a (re)registering worker's running tasks: adopt the
    /// still-assigned ones (through the write path), fence the rest, and
    /// release the tasks the coordinator still believes this worker runs
    /// but it did NOT report — the truth source for restart recovery.
    async fn reattach_tasks(&self, worker_id: &str, running: Vec<String>) {
        let (adopt, preempted) = self.coordinator.classify_running_tasks(worker_id, &running);
        if !adopt.is_empty() {
            let cmd = Command::AdoptTasks {
                worker_id: worker_id.to_string(),
                task_ids: adopt,
            };
            if let Err(e) = self.writes.propose(cmd).await {
                warn!("AdoptTasks proposal failed: {}", e);
            }
        }
        for task_id in preempted {
            self.coordinator.queue_preemption(worker_id, &task_id);
        }

        // Restart recovery: a restarted worker process registers with an
        // empty (or partial) running list while the Raft-replayed
        // coordinator still holds its tasks in Running/Deploying. Those
        // unreported tasks are lost work of a previous process lifetime;
        // release them so the claim rule can hand them out again (the
        // worker resumes from its local checkpoint store).
        let lost = self
            .coordinator
            .reconcile_lost_active_tasks(worker_id, &running);
        if !lost.is_empty() {
            let task_ids: Vec<String> = lost.iter().map(|t| t.task_id.clone()).collect();
            let cmd = Command::ReleaseLostTasks {
                worker_id: worker_id.to_string(),
                task_ids,
            };
            match self.writes.propose(cmd).await {
                Ok(_) => {
                    for task in &lost {
                        info!(
                            "Restart recovery: task {} of job {} released (worker {} no longer \
                             runs it); it will be re-dispatched and resume from its checkpoint",
                            task.task_id, task.job_id, task.worker_id
                        );
                    }
                }
                Err(e) => warn!("ReleaseLostTasks proposal failed: {}", e),
            }
        }
        self.wake_heartbeats();
    }

    /// Cheap recompute for a parked heartbeat that was woken: identical
    /// decisions, empty task list carried in (task metrics already
    /// ingested by the first pass).
    async fn recompute_heartbeat(&self, worker_id: &str) -> HeartbeatResponse {
        let pending_tasks = {
            let claimed = self
                .coordinator
                .claim_tasks_for_worker(worker_id, "", &|w| self.classify(w));
            if self.dispatch_batch_limit > 0 {
                claimed
                    .into_iter()
                    .take(self.dispatch_batch_limit as usize)
                    .collect()
            } else {
                claimed
            }
        };
        if !pending_tasks.is_empty() {
            let ids: Vec<String> = pending_tasks.iter().map(|t| t.task_id.clone()).collect();
            let cmd = Command::MarkDispatched {
                task_ids: ids,
                worker_id: worker_id.to_string(),
            };
            if let Err(e) = self.writes.propose(cmd).await {
                warn!("MarkDispatched proposal failed: {}", e);
            }
        }
        let checkpoint_triggers = self.coordinator.deliver_checkpoint_triggers(worker_id);
        HeartbeatResponse {
            worker_id: worker_id.to_string(),
            next_interval_ms: self.heartbeat_interval_ms as i64,
            pending_tasks,
            cancel_jobs: self.coordinator.cancelled_job_ids(),
            preempted_task_ids: self.coordinator.drain_preemptions(worker_id),
            term: self.coordinator.term(),
            leader_hint: String::new(),
            checkpoint_triggers,
            checkpoint_resolutions: self.coordinator.drain_checkpoint_resolutions(worker_id),
        }
    }

    async fn compute_heartbeat(
        &self,
        hb: HeartbeatRequest,
    ) -> Result<Response<HeartbeatResponse>, Status> {
        let worker_id = hb.worker_id.clone();
        let now = seatunnel_engine_core::now_millis();
        let my_term = self.coordinator.term();

        // Fencing: a worker operating under a HIGHER term means a
        // successor master exists — ratchet our term and stand down.
        if hb.term > my_term {
            warn!(
                "Worker {} reports term {} > ours {} — possible stale master; standing down \
                 dispatch this round",
                worker_id, hb.term, my_term
            );
            self.coordinator.observe_term(hb.term);
            return Ok(Response::new(HeartbeatResponse {
                worker_id,
                next_interval_ms: self.heartbeat_interval_ms as i64,
                pending_tasks: Vec::new(),
                cancel_jobs: Vec::new(),
                preempted_task_ids: Vec::new(),
                term: self.coordinator.term(),
                leader_hint: String::new(),
                checkpoint_triggers: Vec::new(),
                checkpoint_resolutions: Vec::new(),
            }));
        }
        // Leadership gate (consensus mode): a follower master serves no
        // instructions; point the worker at the leader.
        if !self.writes.is_leader() {
            return Ok(Response::new(HeartbeatResponse {
                worker_id,
                next_interval_ms: self.heartbeat_interval_ms as i64,
                pending_tasks: Vec::new(),
                cancel_jobs: Vec::new(),
                preempted_task_ids: Vec::new(),
                term: self.coordinator.term(),
                leader_hint: self.writes.leader_hint(),
                checkpoint_triggers: Vec::new(),
                checkpoint_resolutions: Vec::new(),
            }));
        }

        // A worker evicted by TTL that comes back (SIGSTOP/network
        // glitch) may still run tasks that were reassigned in its
        // absence — fence them from its heartbeat task list.
        {
            let known = {
                let reg = self.workers.read().unwrap();
                reg.contains_key(&worker_id)
            };
            if !known && !hb.tasks.is_empty() {
                let running: Vec<String> = hb.tasks.iter().map(|t| t.task_id.clone()).collect();
                warn!(
                    "Worker {} heartbeating before registration; re-attaching {} task(s)",
                    worker_id,
                    running.len()
                );
                self.reattach_tasks(&worker_id, running).await;
            }
        }

        // Live per-task metrics shipped with the heartbeat: record
        // counters, last-record timestamp and log increments.
        for task in &hb.tasks {
            self.coordinator.report_task_metrics(
                &task.task_id,
                task.processed_records.max(0) as u64,
                task.last_record_at,
                task.logs.clone(),
                &hb.worker_id,
                task.last_checkpoint_id.max(0) as u64,
                task.last_checkpoint_size_bytes.max(0) as u64,
                task.sink_metrics.as_ref().map(|m| m.into()),
            );
        }

        // Cancel reconciliation: the heartbeat task list is the set the
        // worker actually runs, so a cancelled job's `Running` task that
        // is absent from its owner's heartbeat had its terminal report
        // lost — synthesize the CANCELLED report so the cancel broadcast
        // stops instead of riding every heartbeat forever.
        let reported: Vec<String> = hb.tasks.iter().map(|t| t.task_id.clone()).collect();
        for stuck in self
            .coordinator
            .reconcile_lost_cancelled_tasks(&worker_id, &reported)
        {
            info!(
                "Cancel reconcile: task {} of cancelled job {} no longer on worker {} — \
                 synthesizing CANCELLED report",
                stuck.task_id, stuck.job_id, stuck.worker_id
            );
            let cmd = Command::TaskStatus {
                job_id: stuck.job_id,
                task_id: stuck.task_id,
                worker_id: stuck.worker_id,
                state: "CANCELLED".to_string(),
                records: stuck.processed_records,
                error: None,
            };
            if let Err(e) = self.writes.propose(cmd).await {
                warn!("cancel reconcile: TaskStatus proposal failed: {}", e);
            }
        }

        // Restart recovery (heartbeat): release Running tasks this worker
        // stopped reporting long ago — see
        // [`Self::reconcile_heartbeat_lost_tasks`]. Leader-only: it sits
        // behind the leadership gate above.
        let heartbeat_lost = self.reconcile_heartbeat_lost_tasks(&worker_id, &reported, now);
        if !heartbeat_lost.is_empty() {
            let task_ids: Vec<String> = heartbeat_lost.iter().map(|t| t.task_id.clone()).collect();
            warn!(
                "Restart recovery: task(s) {} still Running on worker {} in the coordinator but \
                 absent from its heartbeats for >{}ms — releasing for re-dispatch (resume from \
                 checkpoint)",
                task_ids.join(", "),
                worker_id,
                self.lost_grace_ms
            );
            let cmd = Command::ReleaseLostTasks {
                worker_id: worker_id.clone(),
                task_ids,
            };
            if let Err(e) = self.writes.propose(cmd).await {
                warn!("restart recovery: ReleaseLostTasks proposal failed: {}", e);
            }
        }

        // Refresh liveness + admission signals; a worker returning from a
        // silence longer than the soft timeout, or one reporting it is
        // over a pressure watermark, gets no NEW assignments this round
        // (running tasks are untouched either way).
        let mut soft_stale = false;
        {
            let mut reg = self.workers.write().unwrap();
            match reg.get_mut(&worker_id) {
                Some(entry) => {
                    soft_stale = now - entry.last_heartbeat_ms > self.worker_soft_timeout_ms as i64;
                    entry.last_heartbeat_ms = now;
                    entry.load_score = hb.load_score;
                    entry.lag_ms = hb.lag_ms;
                    entry.mem_permille = hb.mem_permille;
                    entry.cpu_permille = hb.cpu_permille;
                    entry.can_accept = hb.can_accept;
                }
                None => {
                    // Heartbeat before registration — accept it anyway so a
                    // restarted worker recovers without a full re-register.
                    let mut entry = WorkerEntry::new(hb.address.clone());
                    entry.load_score = hb.load_score;
                    entry.lag_ms = hb.lag_ms;
                    entry.mem_permille = hb.mem_permille;
                    entry.cpu_permille = hb.cpu_permille;
                    entry.can_accept = hb.can_accept;
                    reg.insert(worker_id.clone(), entry);
                }
            }
        }
        let admission_blocked = !hb.can_accept;
        if soft_stale {
            warn!(
                "Worker {} silent > {}ms (soft timeout): skipping new assignments this round",
                worker_id, self.worker_soft_timeout_ms
            );
        }
        if admission_blocked {
            warn!(
                "Worker {} over admission watermark (score {}‰, lag {}ms, mem {}‰): \\
                 no new assignments; pending tasks may be stolen",
                worker_id, hb.load_score, hb.lag_ms, hb.mem_permille
            );
        }

        // Failover-aware handout: own pending tasks plus orphans of dead
        // workers plus PENDING tasks of overloaded ones. The claim
        // decision is read-only; the durable mutation is a MarkDispatched
        // command (never steals confirmed-RUNNING tasks).
        let mut pending_tasks = if soft_stale || admission_blocked {
            Vec::new()
        } else {
            let claimed = self
                .coordinator
                .claim_tasks_for_worker(&worker_id, &hb.address, &|w| self.classify(w));
            // Rate fuse for the admission blind window.
            if self.dispatch_batch_limit > 0 {
                claimed
                    .into_iter()
                    .take(self.dispatch_batch_limit as usize)
                    .collect()
            } else {
                claimed
            }
        };
        if !pending_tasks.is_empty() {
            let ids: Vec<String> = pending_tasks.iter().map(|t| t.task_id.clone()).collect();
            let cmd = Command::MarkDispatched {
                task_ids: ids,
                worker_id: worker_id.clone(),
            };
            // Deliver only what durably transferred: `mark_tasks_dispatched`
            // skips tasks still confirmed-RUNNING under another worker (the
            // claim of a dead owner's task before eviction), and delivering
            // such a task would only get the receiver's RUNNING report
            // fenced as a duplicate.
            let transferred = match self.writes.propose(cmd).await {
                Ok(_) => {
                    pending_tasks.retain(|t| {
                        let owned = self.coordinator.task_owned_by(&t.task_id, &worker_id);
                        if !owned {
                            info!(
                                "Skipping dispatch of {} to {}: ownership transfer was skipped",
                                t.task_id, worker_id
                            );
                        }
                        owned
                    });
                    true
                }
                Err(e) => {
                    warn!("MarkDispatched proposal failed: {}", e);
                    false
                }
            };
            if transferred && !pending_tasks.is_empty() {
                info!(
                    "Dispatching {} task(s) to worker {}",
                    pending_tasks.len(),
                    worker_id
                );
            } else {
                pending_tasks.clear();
            }
        }

        // Coordinated checkpoints: propose due triggers (the master is the
        // checkpoint driver), then deliver on this worker's heartbeat.
        if !soft_stale {
            let now = seatunnel_engine_core::now_millis();
            for (job_id, stage_id) in self.coordinator.due_checkpoint_stages() {
                let cmd = Command::CheckpointTriggered {
                    job_id,
                    stage_id,
                    at_ms: now,
                };
                if let Err(e) = self.writes.propose(cmd).await {
                    warn!("CheckpointTriggered proposal failed: {}", e);
                }
            }
        }
        let checkpoint_triggers = if soft_stale {
            Vec::new()
        } else {
            self.coordinator.deliver_checkpoint_triggers(&worker_id)
        };
        let checkpoint_resolutions = self.coordinator.drain_checkpoint_resolutions(&worker_id);

        // Push cancellations so workers stop their local tasks promptly.
        let cancel_jobs = self.coordinator.cancelled_job_ids();

        // Preemption fence: tasks reassigned away from this worker.
        let preempted_task_ids = self.coordinator.drain_preemptions(&worker_id);

        Ok(Response::new(HeartbeatResponse {
            worker_id,
            next_interval_ms: self.heartbeat_interval_ms as i64,
            pending_tasks,
            cancel_jobs,
            preempted_task_ids,
            term: self.coordinator.term(),
            // Empty hint = this node is the active master.
            leader_hint: String::new(),
            checkpoint_triggers,
            checkpoint_resolutions,
        }))
    }
}

/// Whether a heartbeat response carries nothing the worker must act on.
fn response_has_nothing(r: &HeartbeatResponse) -> bool {
    r.pending_tasks.is_empty()
        && r.cancel_jobs.is_empty()
        && r.preempted_task_ids.is_empty()
        && r.checkpoint_triggers.is_empty()
        && r.checkpoint_resolutions.is_empty()
}

#[tonic::async_trait]
impl MasterService for MasterHandler {
    async fn register_worker(
        &self,
        request: Request<WorkerRegistration>,
    ) -> Result<Response<WorkerRegistrationResponse>, Status> {
        let reg = request.into_inner();
        info!(
            "Worker {} registering at {} ({} running task(s), slots={})",
            reg.worker_id,
            reg.address,
            reg.running_task_ids.len(),
            reg.slots
        );
        // Adopt-first: tasks still assigned to this worker are re-marked
        // Running for it (re-attach); only reassigned tasks get fenced.
        self.reattach_tasks(&reg.worker_id, reg.running_task_ids.clone())
            .await;
        self.workers
            .write()
            .unwrap()
            .insert(reg.worker_id.clone(), WorkerEntry::new(reg.address.clone()));

        // Workers should heartbeat the leader: a follower answering with
        // its own address would park them on a node that cannot dispatch.
        let leader_address = if self.writes.is_leader() {
            self.info.advertise_addr.clone()
        } else {
            let hint = self.writes.leader_hint();
            if hint.is_empty() {
                self.info.advertise_addr.clone()
            } else {
                hint
            }
        };
        Ok(Response::new(WorkerRegistrationResponse {
            success: true,
            message: "registered".to_string(),
            leader_address,
            term: self.coordinator.term(),
        }))
    }

    async fn heartbeat(
        &self,
        request: Request<HeartbeatRequest>,
    ) -> Result<Response<HeartbeatResponse>, Status> {
        let hb = request.into_inner();
        let wait_ms = hb.wait_ms.max(0).min(10_000) as u64;

        // Long-poll: compute the response; if there is nothing to deliver
        // and the worker asked to wait, park on the wake signal until an
        // event fires or the wait budget runs out, then recompute once.
        let response = self.compute_heartbeat(hb).await?;
        if wait_ms == 0 {
            return Ok(response);
        }
        let inner = response.into_inner();
        if !response_has_nothing(&inner) {
            return Ok(Response::new(inner));
        }
        let worker_id = inner.worker_id.clone();
        let mut notified = std::pin::pin!(self.wake.notified());
        let deadline = tokio::time::Instant::now() + std::time::Duration::from_millis(wait_ms);
        let woken = tokio::select! {
            _ = notified.as_mut() => true,
            _ = tokio::time::sleep_until(deadline) => false,
        };
        let inner = if woken {
            self.recompute_heartbeat(&worker_id).await
        } else {
            inner
        };
        Ok(Response::new(inner))
    }

    async fn report_task_status(
        &self,
        request: Request<TaskStatusReport>,
    ) -> Result<Response<Empty>, Status> {
        let report = request.into_inner();
        let mut state = self.state.lock().await;
        state.task_assignments.remove(&report.task_id);
        drop(state);

        let state_str = match report.state {
            0 | 1 => "CREATED",
            2 => "RUNNING",
            3 => "COMPLETED",
            4 => "FAILED",
            5 => "CANCELLED",
            _ => "UNKNOWN",
        };
        info!(
            "Task {} reported {} (records={})",
            report.task_id,
            JobState::from(state_str).to_wire(),
            report.processed_records
        );
        let cmd = Command::TaskStatus {
            job_id: report.job_id.clone(),
            task_id: report.task_id.clone(),
            worker_id: report.worker_id.clone(),
            state: state_str.to_string(),
            records: report.processed_records.max(0) as u64,
            error: if report.error_message.is_empty() {
                None
            } else {
                Some(report.error_message)
            },
        };
        if let Err(e) = self.writes.propose(cmd).await {
            // Reply with a retryable error: the worker's terminal-report
            // loop treats any failure as "not delivered" and retries.
            // Replying Ok here would ack a LOST terminal transition and
            // pin the job in RUNNING forever.
            warn!("TaskStatus proposal failed: {}", e);
            return Err(tonic::Status::unavailable(format!(
                "task status proposal failed: {e}"
            )));
        }
        // A terminal transition may unblock other dispatch decisions.
        self.wake_heartbeats();

        Ok(Response::new(Empty {}))
    }

    async fn report_checkpoint(
        &self,
        request: Request<CheckpointReport>,
    ) -> Result<Response<Empty>, Status> {
        let report = request.into_inner();
        tracing::debug!(
            "Checkpoint {} for job {} task {} phase={:?} success={} ({} bytes)",
            report.checkpoint_id,
            report.job_id,
            report.task_id,
            report.phase,
            report.success,
            report.checkpoint_data.len()
        );
        // Master-backed shared store: persist the uploaded bytes so any
        // worker can resume this task after a failover.
        if report.success && !report.checkpoint_data.is_empty() {
            self.coordinator
                .checkpoint_store()
                .save(
                    &report.job_id,
                    &report.task_id,
                    report.checkpoint_id.max(0) as u64,
                    &report.checkpoint_data,
                )
                .await;
        }
        // Exit-time final barriers are pure state flushes (the task is
        // leaving); they never join a coordinated checkpoint.
        let is_final = report.checkpoint_id as u64
            == seatunnel_engine_core::local_checkpoint::FINAL_CHECKPOINT_ID;
        if !is_final
            && report.phase == seatunnel_engine_comm::CheckpointPhase::CheckpointPrepare as i32
        {
            if let Some((stage_id, completed, participants)) =
                self.coordinator.note_checkpoint_prepare(
                    &report.job_id,
                    &report.task_id,
                    report.checkpoint_id.max(0) as u64,
                    report.success,
                )
            {
                let cmd = Command::CheckpointResolved {
                    job_id: report.job_id.clone(),
                    stage_id,
                    checkpoint_id: report.checkpoint_id.max(0) as u64,
                    completed,
                    participants,
                };
                if let Err(e) = self.writes.propose(cmd).await {
                    warn!("CheckpointResolved proposal failed: {}", e);
                }
                // Resolutions are waiting for their workers' heartbeats.
                self.wake_heartbeats();
            }
        } else if !is_final {
            // Legacy interval-path report (pre-coordination senders):
            // count it so dashboards keep working.
            self.coordinator.report_checkpoint(
                &report.job_id,
                &report.task_id,
                report.checkpoint_id.max(0) as u64,
                report.success,
            );
        }
        Ok(Response::new(Empty {}))
    }

    async fn fetch_checkpoint(
        &self,
        request: Request<FetchCheckpointRequest>,
    ) -> Result<Response<FetchCheckpointResponse>, Status> {
        let req = request.into_inner();
        // Heartbeat-reported history: the highest checkpoint id the
        // master ever recorded for this task, even when no restorable
        // payload exists (drives the worker's restore-missing policy).
        let latest = self
            .coordinator
            .get_job(&req.job_id)
            .map(|job| {
                job.tasks
                    .get(&req.task_id)
                    .map(|info| info.last_checkpoint_id as i64)
                    .unwrap_or(0)
            })
            .unwrap_or(0);
        match self
            .coordinator
            .fetch_checkpoint(&req.job_id, &req.task_id)
            .await
        {
            Some((id, data)) => Ok(Response::new(FetchCheckpointResponse {
                checkpoint_id: id as i64,
                checkpoint_data: data,
                latest_checkpoint_id: latest,
            })),
            None => Ok(Response::new(FetchCheckpointResponse {
                checkpoint_id: 0,
                checkpoint_data: Vec::new(),
                latest_checkpoint_id: latest,
            })),
        }
    }

    /// Minimal job-state probe for workers (the richer console variant
    /// lives on the client service).
    async fn get_job_status(
        &self,
        request: Request<JobStatusRequest>,
    ) -> Result<Response<JobStatus>, Status> {
        let req = request.into_inner();
        let job = self
            .coordinator
            .get_job(&req.job_id)
            .ok_or_else(|| Status::not_found(format!("Job {} not found", req.job_id)))?;
        Ok(Response::new(JobStatus {
            job_id: job.job_id.clone(),
            state: job.state.to_proto_state(),
            job_name: job.job_name.clone(),
            ..Default::default()
        }))
    }

    async fn unregister_worker(
        &self,
        request: Request<UnregisterWorkerRequest>,
    ) -> Result<Response<Empty>, Status> {
        let req = request.into_inner();
        warn!("Worker unregistering: {}", req.worker_id);
        self.workers.write().unwrap().remove(&req.worker_id);
        // Graceful shutdown must actually RELEASE the worker's tasks for
        // failover — without this a clean restart waits for the hard
        // eviction timeout before another worker can take over.
        let cmd = Command::EvictWorker {
            worker_id: req.worker_id.clone(),
        };
        if let Err(e) = self.writes.propose(cmd).await {
            warn!("EvictWorker proposal failed: {}", e);
        }
        // The released tasks are claimable — wake the parked heartbeats.
        self.wake_heartbeats();
        let affected: Vec<String> = Vec::new();
        if !affected.is_empty() {
            info!(
                "Worker {} unregistered: {} task(s) released for takeover",
                req.worker_id,
                affected.len()
            );
        }
        Ok(Response::new(Empty {}))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_registry_roundtrip() {
        let registry = new_worker_registry();
        assert!(registry_snapshot(&registry).is_empty());
        registry.write().unwrap().insert("w1".into(), {
            let mut e = WorkerEntry::new("127.0.0.1:5001".into());
            e.last_heartbeat_ms = 0;
            e
        });
        assert_eq!(
            registry_snapshot(&registry),
            vec![("w1".to_string(), "127.0.0.1:5001".to_string())]
        );
    }

    #[test]
    fn heartbeat_grace_releases_absent_running_tasks_only_after_window() {
        // Regression: a hybrid node's embedded worker registers while the
        // Raft log is still replaying, so the registration-time reconcile
        // sees an empty coordinator — and tasks restored afterwards stay
        // Running on a Healthy worker forever (nothing executes them).
        // The heartbeat-time grace reconcile is the self-heal path.
        let coordinator = Arc::new(JobCoordinator::new());
        let config = serde_json::json!({
            "env": { "parallelism": 1 },
            "source": { "Fake": {} },
            "sink": { "Console": {} }
        });
        let (_, tasks) = coordinator
            .compile_and_install(
                "jgrace",
                "job-grace",
                &config,
                None,
                &[(
                    "worker-0".to_string(),
                    "127.0.0.1:5001".to_string(),
                    100,
                    true,
                )],
            )
            .unwrap();
        let task_id = tasks[0].task_id.clone();
        coordinator.mark_tasks_dispatched(std::slice::from_ref(&task_id), "worker-0");
        coordinator.report_task_status("jgrace", &task_id, "RUNNING", 0, None);

        let handler = MasterHandler::new_direct(
            coordinator,
            new_worker_registry(),
            MasterInfo {
                advertise_addr: "127.0.0.1:5800".to_string(),
                role: "hybrid".to_string(),
            },
            1000,
            60_000,
        );

        // Restart simulation: the worker reports nothing. The task is
        // missing from the first heartbeat but is only released once the
        // grace window (30s floor) has fully elapsed.
        let absent: Vec<String> = Vec::new();
        assert!(
            handler
                .reconcile_heartbeat_lost_tasks("worker-0", &absent, 0)
                .is_empty()
        );
        assert!(
            handler
                .reconcile_heartbeat_lost_tasks("worker-0", &absent, 29_999)
                .is_empty()
        );
        let due = handler.reconcile_heartbeat_lost_tasks("worker-0", &absent, 30_000);
        assert_eq!(due.len(), 1, "missing past the grace window must be due");
        assert_eq!(due[0].task_id, task_id);

        // Presence resets tracking: after the task is reported running
        // again, a fresh absence starts a new grace window instead of
        // firing immediately — it elapses again 30s after the report.
        let reported = vec![task_id.clone()];
        assert!(
            handler
                .reconcile_heartbeat_lost_tasks("worker-0", &reported, 60_000)
                .is_empty()
        );
        assert!(
            handler
                .reconcile_heartbeat_lost_tasks("worker-0", &absent, 60_001)
                .is_empty()
        );
        assert!(
            handler
                .reconcile_heartbeat_lost_tasks("worker-0", &absent, 90_000)
                .is_empty()
        );
        assert_eq!(
            handler
                .reconcile_heartbeat_lost_tasks("worker-0", &absent, 90_001)
                .len(),
            1
        );
    }
}
