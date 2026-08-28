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
    HeartbeatResponse, MasterService, TaskStatusReport, UnregisterWorkerRequest,
    WorkerRegistration, WorkerRegistrationResponse,
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
pub fn registry_snapshot_admission(
    registry: &WorkerRegistry,
) -> Vec<(String, String, u32, bool)> {
    registry
        .read()
        .unwrap()
        .iter()
        .map(|(id, e)| {
            (
                id.clone(),
                e.address.clone(),
                e.load_score,
                e.can_accept,
            )
        })
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
        MasterHandler {
            state: Mutex::new(MasterState::default()),
            coordinator,
            workers,
            writes,
            wake: Arc::new(tokio::sync::Notify::new()),
            info,
            heartbeat_interval_ms: heartbeat_interval_ms.clamp(250, 60_000),
            worker_soft_timeout_ms: worker_soft_timeout_ms.max(1_000),
            dispatch_batch_limit: 16,
        }
    }

    /// Override the per-heartbeat dispatch batch limit.
    pub fn with_dispatch_batch_limit(mut self, limit: u32) -> Self {
        self.dispatch_batch_limit = limit;
        self
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
    /// still-assigned ones (through the write path), fence the rest.
    async fn reattach_tasks(&self, worker_id: &str, running: Vec<String>) {
        if running.is_empty() {
            return;
        }
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
            let ids: Vec<String> =
                pending_tasks.iter().map(|t| t.task_id.clone()).collect();
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
            checkpoint_resolutions: self
                .coordinator
                .drain_checkpoint_resolutions(worker_id),
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
            );
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
                    entry.can_accept = hb.can_accept;
                }
                None => {
                    // Heartbeat before registration — accept it anyway so a
                    // restarted worker recovers without a full re-register.
                    let mut entry = WorkerEntry::new(hb.address.clone());
                    entry.load_score = hb.load_score;
                    entry.lag_ms = hb.lag_ms;
                    entry.mem_permille = hb.mem_permille;
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
        let pending_tasks = if soft_stale || admission_blocked {
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
            info!(
                "Dispatching {} task(s) to worker {}",
                pending_tasks.len(),
                worker_id
            );
            let ids: Vec<String> = pending_tasks.iter().map(|t| t.task_id.clone()).collect();
            let cmd = Command::MarkDispatched {
                task_ids: ids,
                worker_id: worker_id.clone(),
            };
            if let Err(e) = self.writes.propose(cmd).await {
                warn!("MarkDispatched proposal failed: {}", e);
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
        self.workers.write().unwrap().insert(
            reg.worker_id.clone(),
            WorkerEntry::new(reg.address.clone()),
        );

        Ok(Response::new(WorkerRegistrationResponse {
            success: true,
            message: "registered".to_string(),
            leader_address: self.info.advertise_addr.clone(),
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
        let deadline =
            tokio::time::Instant::now() + std::time::Duration::from_millis(wait_ms);
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
            warn!("TaskStatus proposal failed: {}", e);
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
            if let Some((stage_id, completed, participants)) = self.coordinator.note_checkpoint_prepare(
                &report.job_id,
                &report.task_id,
                report.checkpoint_id.max(0) as u64,
                report.success,
            ) {
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
        match self
            .coordinator
            .fetch_checkpoint(&req.job_id, &req.task_id)
            .await
        {
            Some((id, data)) => Ok(Response::new(FetchCheckpointResponse {
                checkpoint_id: id as i64,
                checkpoint_data: data,
            })),
            None => Ok(Response::new(FetchCheckpointResponse {
                checkpoint_id: 0,
                checkpoint_data: Vec::new(),
            })),
        }
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
}
