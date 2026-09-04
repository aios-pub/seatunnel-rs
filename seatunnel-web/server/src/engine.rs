/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.
 */

//! Engine abstraction used by the REST handlers.
//!
//! [`EngineOps`] mirrors the master's `ClientService` surface so handlers
//! stay unit-testable: production uses the gRPC [`EngineClient`], tests
//! use [`FakeEngine`].

use async_trait::async_trait;
use seatunnel_engine_client::EngineClient;
use seatunnel_engine_comm::{
    CheckpointEntry, ClusterInfo, JobCheckpointHistory, JobList, JobStatus, JobSummary,
};

use crate::dto::{
    CheckpointEntryDto, CheckpointHistoryDto, ClusterInfoDto, JobLogsDto, JobStatusDto,
    JobSummaryDto, SubmitJobDto, SubmitResultDto, TaskCheckpointDto, TaskLogsDto, TaskStatusDto,
    UpdateResultDto, WorkerDto,
};

/// Engine operation failures, mapped to HTTP statuses by the handlers.
#[derive(Debug, thiserror::Error)]
pub enum EngineError {
    #[error("{0}")]
    NotFound(String),
    #[error("{0}")]
    Invalid(String),
    #[error("master unreachable: {0}")]
    Unreachable(String),
    #[error("{0}")]
    Engine(String),
}

impl EngineError {
    /// HTTP status for this error class.
    pub fn http_status(&self) -> axum::http::StatusCode {
        use axum::http::StatusCode;
        match self {
            EngineError::NotFound(_) => StatusCode::NOT_FOUND,
            EngineError::Invalid(_) => StatusCode::BAD_REQUEST,
            EngineError::Unreachable(_) => StatusCode::SERVICE_UNAVAILABLE,
            EngineError::Engine(_) => StatusCode::BAD_GATEWAY,
        }
    }

    /// Classify a boxed `EngineClient` error (gRPC status or transport error).
    fn from_client(err: Box<dyn std::error::Error + Send + Sync>) -> Self {
        if let Some(status) = err.downcast_ref::<tonic::Status>() {
            return match status.code() {
                tonic::Code::NotFound => EngineError::NotFound(status.message().to_string()),
                tonic::Code::InvalidArgument | tonic::Code::FailedPrecondition => {
                    EngineError::Invalid(status.message().to_string())
                }
                tonic::Code::Unavailable => EngineError::Unreachable(status.message().to_string()),
                _ => EngineError::Engine(status.message().to_string()),
            };
        }
        // Connect failures from EngineClient's failover loop surface here.
        let msg = err.to_string();
        if msg.contains("no reachable master") || msg.contains("transport error") {
            EngineError::Unreachable(msg)
        } else {
            EngineError::Engine(msg)
        }
    }
}

/// Name for a proto `JobState` code (1..=7).
pub fn job_state_name(code: i32) -> &'static str {
    match code {
        1 => "CREATED",
        2 => "SCHEDULED",
        3 => "RUNNING",
        4 => "COMPLETED",
        5 => "FAILED",
        6 => "CANCELLED",
        7 => "DEPLOYING",
        _ => "UNKNOWN",
    }
}

/// Name for a proto `TaskState` code (1..=5).
fn task_state_name(code: i32) -> &'static str {
    match code {
        1 => "CREATED",
        2 => "RUNNING",
        3 => "COMPLETED",
        4 => "FAILED",
        5 => "CANCELLED",
        _ => "UNKNOWN",
    }
}

/// Engine operations consumed by the web console.
#[async_trait]
pub trait EngineOps: Send + Sync {
    async fn list_jobs(&self) -> Result<Vec<JobSummaryDto>, EngineError>;
    async fn job_status(&self, job_id: &str) -> Result<JobStatusDto, EngineError>;
    async fn submit_job(
        &self,
        job: SubmitJobDto,
        job_id: String,
        config_bytes: Vec<u8>,
    ) -> Result<SubmitResultDto, EngineError>;
    async fn cancel_job(&self, job_id: &str) -> Result<(), EngineError>;
    /// Restart a historical job with its retained config (same id):
    /// cancel (exit checkpoint) when still non-terminal → resubmit;
    /// tasks resume from their last checkpoint.
    async fn restart_job(&self, job_id: &str) -> Result<SubmitResultDto, EngineError>;
    /// Delete a TERMINAL job from history (state + checkpoint metadata).
    /// Non-terminal jobs are rejected by the engine.
    async fn delete_job(&self, job_id: &str) -> Result<(), EngineError>;
    /// Edit-and-restart: cancel (exit checkpoint) → resubmit same id.
    async fn update_job(
        &self,
        job_id: &str,
        job_name: &str,
        config_bytes: Vec<u8>,
        parallelism: i32,
        cancel_timeout_secs: u64,
    ) -> Result<UpdateResultDto, EngineError>;
    async fn cluster_info(&self) -> Result<ClusterInfoDto, EngineError>;
    async fn job_checkpoints(&self, job_id: &str) -> Result<CheckpointHistoryDto, EngineError>;
    async fn job_logs(&self, job_id: &str) -> Result<JobLogsDto, EngineError>;
}

fn summary_dto(j: JobSummary) -> JobSummaryDto {
    JobSummaryDto {
        job_id: j.job_id,
        job_name: j.job_name,
        state: job_state_name(j.state).to_string(),
        start_time_ms: j.start_time,
        end_time_ms: j.end_time,
    }
}

fn status_dto(s: JobStatus) -> JobStatusDto {
    JobStatusDto {
        job_id: s.job_id,
        job_name: s.job_name,
        state: job_state_name(s.state).to_string(),
        start_time_ms: s.start_time,
        end_time_ms: s.end_time,
        error_message: s.error_message,
        checkpoint_interval_ms: s.checkpoint_interval_ms,
        checkpoints_completed: s.checkpoints_completed,
        job_config: s.job_config,
        tasks: s
            .tasks
            .into_iter()
            .map(|t| TaskStatusDto {
                task_id: t.task_id,
                stage_id: t.stage_id,
                state: task_state_name(t.state).to_string(),
                worker_id: t.worker_id,
                processed_records: t.processed_records,
                last_record_ms: t.last_record_at,
                // Filled in by the handler from consecutive samples.
                records_per_sec: 0.0,
                idle_ms: if t.last_record_at > 0 { 0 } else { -1 },
                sink_metrics: t.sink_metrics.as_ref().map(|m| crate::dto::SinkMetricsDto {
                    window_secs: m.window_secs,
                    sent: m.sent,
                    delivered: m.delivered,
                    failed: m.failed,
                    in_flight: m.in_flight,
                    latency_ema_ms: m.latency_ema_ms,
                    latency_max_ms: m.latency_max_ms,
                    last_error: m.last_error.clone(),
                    last_error_at: m.last_error_at,
                }),
                error: t.error,
            })
            .collect(),
        parallelism: s.parallelism,
    }
}

fn cluster_dto(c: ClusterInfo) -> ClusterInfoDto {
    ClusterInfoDto {
        leader_id: c.leader_id,
        leader_term: c.term,
        leader_role: c.role,
        available_workers: c.available_workers,
        total_tasks: c.total_tasks,
        running_tasks: c.running_tasks,
        workers: c
            .workers
            .into_iter()
            .map(|w| WorkerDto {
                worker_id: w.worker_id,
                address: w.address,
                last_heartbeat_ms: w.last_heartbeat,
                running_tasks: w.running_tasks,
                load_score_permille: w.load_score,
                lag_ms: w.lag_ms,
                mem_permille: w.mem_permille,
                can_accept: w.can_accept,
                cpu_permille: w.cpu_permille,
                task_ids: w.task_ids,
            })
            .collect(),
        raft_members: c.raft_members,
    }
}

fn checkpoint_dto(h: JobCheckpointHistory) -> CheckpointHistoryDto {
    CheckpointHistoryDto {
        job_id: h.job_id,
        checkpoint_interval_ms: h.checkpoint_interval_ms,
        checkpoints_completed: h.checkpoints_completed,
        tasks: h
            .tasks
            .into_iter()
            .map(|t| TaskCheckpointDto {
                task_id: t.task_id,
                entries: t
                    .entries
                    .into_iter()
                    .map(|e: CheckpointEntry| CheckpointEntryDto {
                        checkpoint_id: e.checkpoint_id,
                        size_bytes: e.size_bytes,
                    })
                    .collect(),
            })
            .collect(),
    }
}

#[async_trait]
impl EngineOps for EngineClient {
    async fn list_jobs(&self) -> Result<Vec<JobSummaryDto>, EngineError> {
        let JobList { jobs } = self.list_jobs().await.map_err(EngineError::from_client)?;
        Ok(jobs.into_iter().map(summary_dto).collect())
    }

    async fn job_status(&self, job_id: &str) -> Result<JobStatusDto, EngineError> {
        let status = self
            .get_job_status(job_id)
            .await
            .map_err(EngineError::from_client)?;
        Ok(status_dto(status))
    }

    async fn submit_job(
        &self,
        job: SubmitJobDto,
        job_id: String,
        config_bytes: Vec<u8>,
    ) -> Result<SubmitResultDto, EngineError> {
        let name = job
            .job_name
            .unwrap_or_else(|| format!("job-{}", &job_id[4..11]));
        let resp = self
            .submit_job(&job_id, &name, config_bytes, job.parallelism.unwrap_or(0))
            .await
            .map_err(EngineError::from_client)?;
        if !resp.success {
            return Err(EngineError::Invalid(resp.message));
        }
        Ok(SubmitResultDto {
            job_id: resp.job_id,
            message: resp.message,
        })
    }

    async fn cancel_job(&self, job_id: &str) -> Result<(), EngineError> {
        EngineClient::cancel_job(self, job_id)
            .await
            .map_err(EngineError::from_client)
    }

    async fn restart_job(&self, job_id: &str) -> Result<SubmitResultDto, EngineError> {
        let resp = EngineClient::restart_job(self, job_id)
            .await
            .map_err(EngineError::from_client)?;
        if !resp.success {
            return Err(EngineError::Invalid(resp.message));
        }
        Ok(SubmitResultDto {
            job_id: resp.job_id,
            message: resp.message,
        })
    }

    async fn delete_job(&self, job_id: &str) -> Result<(), EngineError> {
        EngineClient::delete_job(self, job_id)
            .await
            .map_err(EngineError::from_client)
    }

    async fn update_job(
        &self,
        job_id: &str,
        job_name: &str,
        config_bytes: Vec<u8>,
        parallelism: i32,
        cancel_timeout_secs: u64,
    ) -> Result<UpdateResultDto, EngineError> {
        // The shared flow (also used by `seatunnel job update`): cancel
        // with exit checkpoint → wait CANCELLED → settle → resubmit the
        // SAME id (checkpoint restore). Aborts without resubmitting on
        // cancel timeout (never run old and new in parallel).
        let options = seatunnel_engine_client::UpdateOptions {
            cancel_timeout_secs,
            ..Default::default()
        };
        let outcome = seatunnel_engine_client::update_job(
            self,
            job_id,
            job_name,
            config_bytes,
            parallelism,
            &options,
        )
        .await
        .map_err(|e| EngineError::Invalid(e.to_string()))?;
        Ok(UpdateResultDto {
            job_id: outcome.job_id,
            cancelled: outcome.cancelled,
            cancel_wait_ms: outcome.cancel_wait_ms as u64,
            message: outcome.message,
        })
    }

    async fn cluster_info(&self) -> Result<ClusterInfoDto, EngineError> {
        let info = self
            .get_cluster_info()
            .await
            .map_err(EngineError::from_client)?;
        Ok(cluster_dto(info))
    }

    async fn job_checkpoints(&self, job_id: &str) -> Result<CheckpointHistoryDto, EngineError> {
        let history = self
            .get_job_checkpoints(job_id)
            .await
            .map_err(EngineError::from_client)?;
        Ok(checkpoint_dto(history))
    }

    async fn job_logs(&self, job_id: &str) -> Result<JobLogsDto, EngineError> {
        let logs = self
            .get_job_logs(job_id)
            .await
            .map_err(EngineError::from_client)?;
        Ok(JobLogsDto {
            job_id: logs.job_id,
            tasks: logs
                .tasks
                .into_iter()
                .map(|t| TaskLogsDto {
                    task_id: t.task_id,
                    lines: t.lines,
                })
                .collect(),
        })
    }
}

#[cfg(test)]
/// In-memory engine used by handler unit tests.
#[derive(Default)]
pub struct FakeEngine {
    pub unreachable: bool,
    pub jobs: std::sync::Mutex<Vec<JobStatusDto>>,
    /// Name passed to the most recent `update_job`, for assertions.
    pub last_update_name: std::sync::Mutex<Option<String>>,
    /// Config bytes passed to the most recent `update_job`, for assertions.
    pub last_update_config: std::sync::Mutex<Option<Vec<u8>>>,
}

#[cfg(test)]
impl FakeEngine {
    pub fn unreachable() -> Self {
        FakeEngine {
            unreachable: true,
            jobs: std::sync::Mutex::new(Vec::new()),
            last_update_name: std::sync::Mutex::new(None),
            last_update_config: std::sync::Mutex::new(None),
        }
    }

    pub fn with_running_job() -> Self {
        let job = JobStatusDto {
            job_id: "job-1".to_string(),
            job_name: "demo".to_string(),
            state: "RUNNING".to_string(),
            start_time_ms: 1,
            end_time_ms: 0,
            error_message: String::new(),
            checkpoint_interval_ms: 10_000,
            checkpoints_completed: 3,
            job_config: r#"{"env":{"job.name":"demo"},"source":{"Fake":{"row.num":1}},"sink":{"Console":{}}}"#.to_string(),
            parallelism: 1,
            tasks: vec![TaskStatusDto {
                task_id: "task-0".to_string(),
                stage_id: "0".to_string(),
                state: "RUNNING".to_string(),
                worker_id: "worker-1".to_string(),
                processed_records: 42,
                last_record_ms: 1,
                records_per_sec: 0.0,
                idle_ms: 0,
                sink_metrics: None,
                error: String::new(),
            }],
        };
        FakeEngine {
            unreachable: false,
            jobs: std::sync::Mutex::new(vec![job]),
            last_update_name: std::sync::Mutex::new(None),
            last_update_config: std::sync::Mutex::new(None),
        }
    }

    fn err(&self) -> EngineError {
        EngineError::Unreachable("no reachable master (fake)".to_string())
    }
}

#[cfg(test)]
#[async_trait]
impl EngineOps for FakeEngine {
    async fn restart_job(&self, job_id: &str) -> Result<SubmitResultDto, EngineError> {
        if self.unreachable {
            return Err(self.err());
        }
        let mut jobs = self.jobs.lock().unwrap();
        let Some(job) = jobs.iter_mut().find(|j| j.job_id == job_id) else {
            return Err(EngineError::NotFound(format!("Job {} not found", job_id)));
        };
        job.state = "CREATED".to_string();
        job.end_time_ms = 0;
        Ok(SubmitResultDto {
            job_id: job_id.to_string(),
            message: "restarted (fake)".to_string(),
        })
    }

    async fn delete_job(&self, job_id: &str) -> Result<(), EngineError> {
        if self.unreachable {
            return Err(self.err());
        }
        let mut jobs = self.jobs.lock().unwrap();
        let Some(job) = jobs.iter().find(|j| j.job_id == job_id) else {
            return Err(EngineError::NotFound(format!("Job {} not found", job_id)));
        };
        if job.state != "COMPLETED" && job.state != "FAILED" && job.state != "CANCELLED" {
            return Err(EngineError::Invalid(format!(
                "job {} is {} — cancel it before deleting",
                job_id, job.state
            )));
        }
        jobs.retain(|j| j.job_id != job_id);
        Ok(())
    }

    async fn update_job(
        &self,
        job_id: &str,
        job_name: &str,
        config_bytes: Vec<u8>,
        _parallelism: i32,
        _cancel_timeout_secs: u64,
    ) -> Result<UpdateResultDto, EngineError> {
        if self.unreachable {
            return Err(self.err());
        }
        *self.last_update_name.lock().unwrap() = Some(job_name.to_string());
        *self.last_update_config.lock().unwrap() = Some(config_bytes);
        Ok(UpdateResultDto {
            job_id: job_id.to_string(),
            cancelled: true,
            cancel_wait_ms: 1,
            message: "updated (fake)".to_string(),
        })
    }

    async fn list_jobs(&self) -> Result<Vec<JobSummaryDto>, EngineError> {
        if self.unreachable {
            return Err(self.err());
        }
        let jobs = self.jobs.lock().unwrap();
        Ok(jobs
            .iter()
            .map(|j| JobSummaryDto {
                job_id: j.job_id.clone(),
                job_name: j.job_name.clone(),
                state: j.state.clone(),
                start_time_ms: j.start_time_ms,
                end_time_ms: j.end_time_ms,
            })
            .collect())
    }

    async fn job_status(&self, job_id: &str) -> Result<JobStatusDto, EngineError> {
        if self.unreachable {
            return Err(self.err());
        }
        self.jobs
            .lock()
            .unwrap()
            .iter()
            .find(|j| j.job_id == job_id)
            .cloned()
            .ok_or_else(|| EngineError::NotFound(format!("Job {} not found", job_id)))
    }

    async fn submit_job(
        &self,
        job: SubmitJobDto,
        job_id: String,
        _config_bytes: Vec<u8>,
    ) -> Result<SubmitResultDto, EngineError> {
        if self.unreachable {
            return Err(self.err());
        }
        let name = job.job_name.unwrap_or_else(|| job_id.clone());
        self.jobs.lock().unwrap().push(JobStatusDto {
            job_id: job_id.clone(),
            job_name: name,
            state: "CREATED".to_string(),
            start_time_ms: 2,
            end_time_ms: 0,
            error_message: String::new(),
            checkpoint_interval_ms: 0,
            checkpoints_completed: 0,
            job_config: String::new(),
            parallelism: 0,
            tasks: Vec::new(),
        });
        Ok(SubmitResultDto {
            job_id,
            message: "scheduled".to_string(),
        })
    }

    async fn cancel_job(&self, job_id: &str) -> Result<(), EngineError> {
        if self.unreachable {
            return Err(self.err());
        }
        let mut jobs = self.jobs.lock().unwrap();
        let Some(job) = jobs.iter_mut().find(|j| j.job_id == job_id) else {
            return Err(EngineError::NotFound(format!("Job {} not found", job_id)));
        };
        job.state = "CANCELLED".to_string();
        Ok(())
    }

    async fn cluster_info(&self) -> Result<ClusterInfoDto, EngineError> {
        if self.unreachable {
            return Err(self.err());
        }
        Ok(ClusterInfoDto {
            leader_term: 1,
            leader_role: "master".to_string(),
            leader_id: "self".to_string(),
            available_workers: 1,
            total_tasks: 1,
            running_tasks: 1,
            raft_members: vec!["127.0.0.1:5800".to_string()],
            workers: vec![WorkerDto {
                load_score_permille: 100,
                lag_ms: 20,
                mem_permille: 300,
                can_accept: true,
                worker_id: "worker-1".to_string(),
                address: "127.0.0.1:5801".to_string(),
                last_heartbeat_ms: 99,
                running_tasks: 1,
                cpu_permille: 120,
                task_ids: vec!["task-0".to_string()],
            }],
        })
    }

    async fn job_checkpoints(&self, job_id: &str) -> Result<CheckpointHistoryDto, EngineError> {
        if self.unreachable {
            return Err(self.err());
        }
        self.jobs
            .lock()
            .unwrap()
            .iter()
            .find(|j| j.job_id == job_id)
            .map(|j| CheckpointHistoryDto {
                job_id: j.job_id.clone(),
                checkpoint_interval_ms: j.checkpoint_interval_ms,
                checkpoints_completed: j.checkpoints_completed,
                tasks: vec![TaskCheckpointDto {
                    task_id: "task-0".to_string(),
                    entries: vec![CheckpointEntryDto {
                        checkpoint_id: 1,
                        size_bytes: 128,
                    }],
                }],
            })
            .ok_or_else(|| EngineError::NotFound(format!("Job {} not found", job_id)))
    }

    async fn job_logs(&self, job_id: &str) -> Result<JobLogsDto, EngineError> {
        if self.unreachable {
            return Err(self.err());
        }
        self.jobs
            .lock()
            .unwrap()
            .iter()
            .find(|j| j.job_id == job_id)
            .map(|j| JobLogsDto {
                job_id: j.job_id.clone(),
                tasks: vec![TaskLogsDto {
                    task_id: "task-0".to_string(),
                    lines: vec![
                        "[00:00:01.000][INFO] task started (job=demo)".to_string(),
                        "[00:00:02.000][DATA] record #100: f0=99".to_string(),
                    ],
                }],
            })
            .ok_or_else(|| EngineError::NotFound(format!("Job {} not found", job_id)))
    }
}
