/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.
 */

//! Engine Client: Submit and manage jobs on a SeaTunnel cluster.
//!
//! Provides gRPC client for job submission and REST API for monitoring.

use reqwest::Client as HttpClient;
use seatunnel_engine_comm::{
    CancelJobRequest, ClientServiceClient, ClusterInfo, JobList, JobStatus, JobStatusRequest,
    SubmitJobRequest, SubmitJobResponse,
};
use tonic::Request;
use tracing::info;

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
        let primary = all.first().cloned().unwrap_or_else(|| master_address.to_string());
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
    ) -> Result<ClientServiceClient<tonic::transport::Channel>, Box<dyn std::error::Error + Send + Sync>>
    {
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
        Err(format!(
            "no reachable master ({})",
            last_err.unwrap_or_default()
        )
        .into())
    }

    /// Try each comma-separated master address in order; returns a client
    /// connected to the first reachable one.
    pub async fn connect_any(
        master_addresses: &str,
    ) -> Result<ClientServiceClient<tonic::transport::Channel>, Box<dyn std::error::Error + Send + Sync>>
    {
        let mut last_err: Option<String> = None;
        for addr in master_addresses.split(',').map(|a| a.trim()).filter(|a| !a.is_empty()) {
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

    /// Submit a job via gRPC.
    pub async fn submit_job(
        &self,
        job_id: &str,
        job_name: &str,
        job_config: Vec<u8>,
        parallelism: i32,
    ) -> Result<SubmitJobResponse, Box<dyn std::error::Error + Send + Sync>> {
        let mut client = self.connect_failover().await?;
        let request = SubmitJobRequest {
            job_id: job_id.to_string(),
            job_config,
            parallelism,
            user: "seatunnel".to_string(),
            job_name: job_name.to_string(),
        };
        let response = client.submit_job(Request::new(request)).await?;
        info!("Job {} submitted successfully", job_id);
        Ok(response.into_inner())
    }

    /// Cancel a job.
    pub async fn cancel_job(
        &self,
        job_id: &str,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let mut client = self.connect_failover().await?;
        let request = CancelJobRequest {
            job_id: job_id.to_string(),
        };
        client.cancel_job(Request::new(request)).await?;
        info!("Job {} cancelled", job_id);
        Ok(())
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

    /// Get REST endpoint URL for monitoring.
    pub fn rest_url(&self, path: &str) -> String {
        format!("{}{}", self.rest_address, path)
    }
}

/// REST API handler for the engine's HTTP monitoring interface.
pub mod rest_api {
    use axum::{routing::get, Router};

    /// Build the REST API router.
    pub fn build_router() -> Router {
        Router::new()
            .route("/api/v1/cluster", get(cluster_info))
            .route("/api/v1/jobs", get(list_jobs))
            .route("/api/v1/health", get(health_check))
    }

    async fn health_check() -> &'static str {
        "OK"
    }

    async fn cluster_info() -> String {
        "{}".to_string()
    }

    async fn list_jobs() -> String {
        "[]".to_string()
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

    #[test]
    fn test_rest_router() {
        let _router = rest_api::build_router();
    }
}
