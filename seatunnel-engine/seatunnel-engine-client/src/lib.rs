/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.
 */

//! Engine Client: Submit and manage jobs on a SeaTunnel cluster.
//!
//! Provides gRPC client for job submission and REST API for monitoring.

pub mod update;

pub use update::{UpdateOptions, UpdateOutcome, update_job};

use reqwest::Client as HttpClient;
use seatunnel_engine_comm::{
    CancelJobRequest, ClientServiceClient, ClusterInfo, JobCheckpointHistory, JobList, JobLogs,
    JobStatus, JobStatusRequest, RestartJobRequest, SubmitJobRequest, SubmitJobResponse,
};
use tonic::Request;
use tracing::info;

/// How long mutating calls keep following leader hints across an
/// election instead of failing (leader failover completes in ~1-3s).
const LEADER_FOLLOW_BUDGET: std::time::Duration = std::time::Duration::from_secs(30);
const LEADER_FOLLOW_DELAY: std::time::Duration = std::time::Duration::from_secs(1);

/// Extract the leader address from the server's leadership-gate error
/// (`"not the leader; retry at <addr>"` — see ClientHandler::require_leader).
/// Returns None when there is no usable hint (no leader elected yet, or
/// an unrelated failure).
fn leader_hint_from_status(status: &tonic::Status) -> Option<String> {
    if status.code() != tonic::Code::FailedPrecondition {
        return None;
    }
    let addr = status.message().strip_prefix("not the leader; retry at ")?;
    if addr.is_empty() || addr == "another master" {
        return None;
    }
    Some(addr.to_string())
}

/// Engine client for submitting and managing jobs.
pub struct EngineClient {
    grpc_address: String,
    /// All master addresses (failover order); comma list from -a/--master.
    all_addresses: Vec<String>,
    rest_address: String,
    #[allow(dead_code)] // reserved for REST-based fallback operations
    http: HttpClient,
}

impl EngineClient {
    /// Create a new engine client pointing to the given master address.
    /// Accepts a comma-separated list (failover order): the first
    /// reachable master is used for every operation.
    pub fn new(master_address: &str) -> Self {
        let all: Vec<String> = master_address
            .split(',')
            .map(|a| a.trim().to_string())
            .filter(|a| !a.is_empty())
            .collect();
        let primary = all
            .first()
            .cloned()
            .unwrap_or_else(|| master_address.to_string());
        EngineClient {
            grpc_address: format!("http://{}", primary),
            all_addresses: all,
            rest_address: format!("http://{}/api/v1", primary),
            http: HttpClient::new(),
        }
    }

    /// Connect to the first reachable master (failover across
    /// `all_addresses`).
    async fn connect_failover(
        &self,
    ) -> Result<
        ClientServiceClient<tonic::transport::Channel>,
        Box<dyn std::error::Error + Send + Sync>,
    > {
        let addresses = if self.all_addresses.is_empty() {
            vec![self.grpc_address.clone()]
        } else {
            self.all_addresses
                .iter()
                .map(|a| format!("http://{}", a))
                .collect()
        };
        let mut last_err: Option<String> = None;
        for addr in addresses {
            match ClientServiceClient::connect(addr.clone()).await {
                Ok(client) => return Ok(client),
                Err(e) => {
                    info!("Master {} unreachable ({}); trying next", addr, e);
                    last_err = Some(format!("{}: {}", addr, e));
                }
            }
        }
        Err(format!("no reachable master ({})", last_err.unwrap_or_default()).into())
    }

    /// Try each comma-separated master address in order; returns a client
    /// connected to the first reachable one.
    pub async fn connect_any(
        master_addresses: &str,
    ) -> Result<
        ClientServiceClient<tonic::transport::Channel>,
        Box<dyn std::error::Error + Send + Sync>,
    > {
        let mut last_err: Option<String> = None;
        for addr in master_addresses
            .split(',')
            .map(|a| a.trim())
            .filter(|a| !a.is_empty())
        {
            match ClientServiceClient::connect(format!("http://{}", addr)).await {
                Ok(client) => return Ok(client),
                Err(e) => {
                    info!("Master {} unreachable ({}); trying next", addr, e);
                    last_err = Some(format!("{}: {}", addr, e));
                }
            }
        }
        Err(format!(
            "no reachable master in '{}' ({})",
            master_addresses,
            last_err.unwrap_or_default()
        )
        .into())
    }

    /// Submit a job via gRPC. Follows the leader when the contacted
    /// master is a follower (leadership-gate hint) and keeps retrying
    /// within LEADER_FOLLOW_BUDGET — a submit issued during failover
    /// succeeds instead of erroring out.
    pub async fn submit_job(
        &self,
        job_id: &str,
        job_name: &str,
        job_config: Vec<u8>,
        parallelism: i32,
    ) -> Result<SubmitJobResponse, Box<dyn std::error::Error + Send + Sync>> {
        let request = SubmitJobRequest {
            job_id: job_id.to_string(),
            job_config,
            parallelism,
            user: "seatunnel".to_string(),
            job_name: job_name.to_string(),
        };
        let response = self
            .with_leader_follow(
                "submit",
                job_id,
                request,
                |mut client, request| async move {
                    client
                        .submit_job(Request::new(request))
                        .await
                        .map(|r| r.into_inner())
                },
            )
            .await?;
        info!("Job {} submitted successfully", job_id);
        Ok(response)
    }

    /// Cancel a job. Follows the leader hint like `submit_job`.
    pub async fn cancel_job(
        &self,
        job_id: &str,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let request = CancelJobRequest {
            job_id: job_id.to_string(),
        };
        self.with_leader_follow(
            "cancel",
            job_id,
            request,
            |mut client, request| async move {
                client.cancel_job(Request::new(request)).await.map(|_| ())
            },
        )
        .await?;
        info!("Job {} cancelled", job_id);
        Ok(())
    }

    /// Restart a historical job with the same id and its retained config.
    /// Follows the leader hint like `submit_job`; long-running by design
    /// (cancels a non-terminal job first — up to the server-side cancel
    /// timeout — then resubmits; tasks resume from their last checkpoint).
    pub async fn restart_job(
        &self,
        job_id: &str,
    ) -> Result<SubmitJobResponse, Box<dyn std::error::Error + Send + Sync>> {
        let request = RestartJobRequest {
            job_id: job_id.to_string(),
        };
        let response = self
            .with_leader_follow(
                "restart",
                job_id,
                request,
                |mut client, request| async move {
                    client
                        .restart_job(Request::new(request))
                        .await
                        .map(|r| r.into_inner())
                },
            )
            .await?;
        info!("Job {} restarted: {}", job_id, response.message);
        Ok(response)
    }

    /// Run a mutating RPC against the configured masters, following the
    /// server's leader hints (and re-trying the configured list when a
    /// hinted leader is unreachable) until the budget expires.
    async fn with_leader_follow<T, R, F, Fut>(
        &self,
        op: &str,
        job_id: &str,
        request: R,
        mut call: F,
    ) -> Result<T, Box<dyn std::error::Error + Send + Sync>>
    where
        R: Clone,
        F: FnMut(ClientServiceClient<tonic::transport::Channel>, R) -> Fut,
        Fut: std::future::Future<Output = Result<T, tonic::Status>>,
    {
        let deadline = tokio::time::Instant::now() + LEADER_FOLLOW_BUDGET;
        // Leader to try next; None = walk the configured failover list.
        let mut hint: Option<String> = None;
        loop {
            let client = match &hint {
                Some(addr) => {
                    match ClientServiceClient::connect(format!("http://{}", addr)).await {
                        Ok(c) => c,
                        Err(e) => {
                            info!(
                                "hinted leader {} unreachable ({}); retrying configured masters",
                                addr, e
                            );
                            if tokio::time::Instant::now() >= deadline {
                                return Err(e.into());
                            }
                            hint = None;
                            tokio::time::sleep(LEADER_FOLLOW_DELAY).await;
                            continue;
                        }
                    }
                }
                None => self.connect_failover().await?,
            };
            match call(client, request.clone()).await {
                Ok(value) => return Ok(value),
                Err(status) => match leader_hint_from_status(&status) {
                    Some(addr) if tokio::time::Instant::now() < deadline => {
                        info!(
                            "{} {}: not the leader; following hint to {}",
                            op, job_id, addr
                        );
                        hint = Some(addr);
                        tokio::time::sleep(LEADER_FOLLOW_DELAY).await;
                    }
                    _ => return Err(status.into()),
                },
            }
        }
    }

    /// Get job status.
    pub async fn get_job_status(
        &self,
        job_id: &str,
    ) -> Result<JobStatus, Box<dyn std::error::Error + Send + Sync>> {
        let mut client = self.connect_failover().await?;
        let request = JobStatusRequest {
            job_id: job_id.to_string(),
        };
        let response = client.get_job_status(Request::new(request)).await?;
        Ok(response.into_inner())
    }

    /// Get cluster info.
    pub async fn get_cluster_info(
        &self,
    ) -> Result<ClusterInfo, Box<dyn std::error::Error + Send + Sync>> {
        use seatunnel_engine_comm::Empty;
        let mut client = self.connect_failover().await?;
        let response = client.get_cluster_info(Request::new(Empty {})).await?;
        Ok(response.into_inner())
    }

    /// List all jobs.
    pub async fn list_jobs(&self) -> Result<JobList, Box<dyn std::error::Error + Send + Sync>> {
        use seatunnel_engine_comm::Empty;
        let mut client = self.connect_failover().await?;
        let response = client.list_jobs(Request::new(Empty {})).await?;
        Ok(response.into_inner())
    }

    /// Get checkpoint history (ids + sizes, no payload bytes) for a job.
    pub async fn get_job_checkpoints(
        &self,
        job_id: &str,
    ) -> Result<JobCheckpointHistory, Box<dyn std::error::Error + Send + Sync>> {
        let mut client = self.connect_failover().await?;
        let request = JobStatusRequest {
            job_id: job_id.to_string(),
        };
        let response = client.get_job_checkpoints(Request::new(request)).await?;
        Ok(response.into_inner())
    }

    /// Get per-task log lines for a job.
    pub async fn get_job_logs(
        &self,
        job_id: &str,
    ) -> Result<JobLogs, Box<dyn std::error::Error + Send + Sync>> {
        let mut client = self.connect_failover().await?;
        let request = JobStatusRequest {
            job_id: job_id.to_string(),
        };
        let response = client.get_job_logs(Request::new(request)).await?;
        Ok(response.into_inner())
    }

    /// Get REST endpoint URL for monitoring.
    pub fn rest_url(&self, path: &str) -> String {
        format!("{}{}", self.rest_address, path)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_client_address() {
        let client = EngineClient::new("127.0.0.1:5000");
        assert_eq!(client.grpc_address, "http://127.0.0.1:5000");
        assert_eq!(
            client.rest_url("/jobs"),
            "http://127.0.0.1:5000/api/v1/jobs"
        );
    }

    fn status(code: tonic::Code, message: &str) -> tonic::Status {
        tonic::Status::new(code, message.to_string())
    }

    #[test]
    fn leader_hint_is_parsed_from_gate_error() {
        let st = status(
            tonic::Code::FailedPrecondition,
            "not the leader; retry at 10.0.0.2:5800",
        );
        assert_eq!(
            leader_hint_from_status(&st),
            Some("10.0.0.2:5800".to_string())
        );
    }

    #[test]
    fn leader_hint_rejected_without_usable_address() {
        // No leader elected yet.
        assert_eq!(
            leader_hint_from_status(&status(
                tonic::Code::FailedPrecondition,
                "not the leader; retry at another master"
            )),
            None
        );
        // Wrong code / unrelated failure.
        assert_eq!(
            leader_hint_from_status(&status(
                tonic::Code::Unavailable,
                "not the leader; retry at 10.0.0.2:5800"
            )),
            None
        );
        assert_eq!(
            leader_hint_from_status(&status(
                tonic::Code::FailedPrecondition,
                "consensus write: timed out"
            )),
            None
        );
    }
}
