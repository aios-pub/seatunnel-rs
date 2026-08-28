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

//! Edit-and-restart: replace a RUNNING job's configuration without data
//! loss, sharing ONE implementation between the CLI (`job update`) and
//! the web console's update entry.
//!
//! Flow (strictly serial — the old and new job NEVER run in parallel,
//! which would double-consume the source):
//! 1. query the job: already terminal / unknown → resubmit directly;
//! 2. cancel; the cancel path takes the exit checkpoint (final sink
//!    flush + source position) automatically — the de-facto savepoint;
//! 3. wait for CANCELLED. On timeout ABORT: never resubmit while the
//!    old incarnation might still be consuming (safety default);
//! 4. settle briefly to drain in-flight terminal reports (the master
//!    also ignores stale terminal reports for undispatched tasks);
//! 5. resubmit with the SAME job id — workers restore from the latest
//!    checkpoint of (job_id, task_id) and continue from the exact
//!    source position (at-least-once; exactly-once with transactional
//!    sinks).

use std::time::Duration;

use tracing::{info, warn};

use crate::EngineClient;

/// Tuning knobs for the update flow.
#[derive(Debug, Clone, Copy)]
pub struct UpdateOptions {
    /// Max seconds to wait for the old job to reach CANCELLED before
    /// aborting (never resubmitting). Big states need longer exit
    /// checkpoints.
    pub cancel_timeout_secs: u64,
    /// Quiet period after CANCELLED is observed, draining in-flight
    /// terminal reports.
    pub settle_ms: u64,
}

impl Default for UpdateOptions {
    fn default() -> Self {
        UpdateOptions {
            cancel_timeout_secs: 60,
            settle_ms: 2_000,
        }
    }
}

/// What happened at each stage, for CLI output and web responses.
#[derive(Debug, Clone)]
pub struct UpdateOutcome {
    pub job_id: String,
    /// True when a cancel was issued (job was running).
    pub cancelled: bool,
    /// Milliseconds the cancel-to-CANCELLED transition took.
    pub cancel_wait_ms: u128,
    pub message: String,
}

/// Proto JobState terminal values.
const STATE_COMPLETED: i32 = 4;
const STATE_FAILED: i32 = 5;
const STATE_CANCELLED: i32 = 6;

/// Replace a job's configuration: cancel (with exit checkpoint) → wait →
/// resubmit with the same id (checkpoint restore).
pub async fn update_job(
    client: &EngineClient,
    job_id: &str,
    job_name: &str,
    new_config: Vec<u8>,
    parallelism: i32,
    options: &UpdateOptions,
) -> Result<UpdateOutcome, Box<dyn std::error::Error + Send + Sync>> {
    let started = std::time::Instant::now();
    let mut cancelled = false;
    let mut cancel_wait_ms = 0;

    match client.get_job_status(job_id).await {
        Ok(status) => {
            let terminal =
                matches!(status.state, STATE_COMPLETED | STATE_FAILED | STATE_CANCELLED);
            if !terminal {
                client.cancel_job(job_id).await?;
                cancelled = true;
                let deadline = tokio::time::Instant::now()
                    + Duration::from_secs(options.cancel_timeout_secs.max(1));
                loop {
                    assert_not_cancelled_timeout(deadline, job_id, options).map_err(
                        |e| -> Box<dyn std::error::Error + Send + Sync> { e },
                    )?;
                    let status = client.get_job_status(job_id).await?;
                    if status.state == STATE_CANCELLED {
                        cancel_wait_ms = started.elapsed().as_millis();
                        info!(
                            "Job {} cancelled (exit checkpoint taken) after {}ms",
                            job_id, cancel_wait_ms
                        );
                        break;
                    }
                    tokio::time::sleep(Duration::from_millis(250)).await;
                }
                // Drain in-flight terminal reports from the old incarnation.
                tokio::time::sleep(Duration::from_millis(options.settle_ms)).await;
            } else {
                info!(
                    "Job {} already terminal (state {}); resubmitting directly",
                    job_id, status.state
                );
            }
        }
        Err(e) => {
            warn!(
                "Job {} not found ({}); treating update as a fresh submit",
                job_id, e
            );
        }
    }

    let response = client
        .submit_job(job_id, job_name, new_config, parallelism)
        .await?;
    if !response.success {
        return Err(format!("resubmission rejected: {}", response.message).into());
    }

    Ok(UpdateOutcome {
        job_id: job_id.to_string(),
        cancelled,
        cancel_wait_ms,
        message: format!(
            "job '{}' resubmitted{}; workers restore from the latest checkpoint",
            job_id,
            if cancelled {
                format!(" after cancel ({}ms)", cancel_wait_ms)
            } else {
                String::new()
            }
        ),
    })
}

fn assert_not_cancelled_timeout(
    deadline: tokio::time::Instant,
    job_id: &str,
    options: &UpdateOptions,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    if tokio::time::Instant::now() >= deadline {
        return Err(format!(
            "job {} did not reach CANCELLED within {}s; update ABORTED without \
             resubmitting (the old job may still be consuming — do not run \
             old and new in parallel; inspect it with `job status` and retry)",
            job_id, options.cancel_timeout_secs
        )
        .into());
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn timeout_error_explains_safety_abort() {
        let deadline = tokio::time::Instant::now();
        let err = assert_not_cancelled_timeout(
            deadline,
            "j1",
            &UpdateOptions {
                cancel_timeout_secs: 5,
                settle_ms: 0,
            },
        )
        .unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("ABORTED"), "must state the abort: {}", msg);
        assert!(msg.contains("parallel"), "must warn about duplicates: {}", msg);
    }
}
