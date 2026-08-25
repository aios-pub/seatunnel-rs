/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.
 */

use crate::master::JobInfo;
use parking_lot::RwLock;
use std::collections::HashMap;

/// Job manager tracks all submitted jobs and their states.
pub struct JobManager {
    jobs: RwLock<HashMap<String, JobInfo>>,
}

impl JobManager {
    pub fn new() -> Self {
        JobManager {
            jobs: RwLock::new(HashMap::new()),
        }
    }

    /// Submit a new job.
    pub fn submit_job(&self, job_id: String, job_name: String, parallelism: i32, start_time: i64) {
        let mut jobs = self.jobs.write();
        jobs.insert(
            job_id.clone(),
            JobInfo { end_time: None, error_message: None,
                job_id,
                job_name,
                state: "CREATED".to_string(),
                parallelism,
                start_time,
            },
        );
    }

    /// Update job state.
    pub fn update_state(&self, job_id: &str, state: String) {
        let mut jobs = self.jobs.write();
        if let Some(info) = jobs.get_mut(job_id) {
            info.state = state;
        }
    }

    /// Cancel a job.
    pub fn cancel_job(&self, job_id: &str) -> bool {
        let mut jobs = self.jobs.write();
        if let Some(info) = jobs.get(job_id) {
            if info.state == "RUNNING" || info.state == "SCHEDULED" || info.state == "CREATED" {
                drop(info);
                if let Some(info) = jobs.get_mut(job_id) {
                    info.state = "CANCELLED".to_string();
                    return true;
                }
            }
        }
        false
    }

    /// Get job info.
    pub fn get_job(&self, job_id: &str) -> Option<JobInfo> {
        let jobs = self.jobs.read();
        jobs.get(job_id).cloned()
    }

    /// List all jobs.
    pub fn list_jobs(&self) -> Vec<JobInfo> {
        let jobs = self.jobs.read();
        jobs.values().cloned().collect()
    }

    /// Remove a completed/failed/cancelled job.
    pub fn remove_job(&self, job_id: &str) {
        let mut jobs = self.jobs.write();
        jobs.remove(job_id);
    }

    /// Count of running jobs.
    pub fn running_count(&self) -> usize {
        let jobs = self.jobs.read();
        jobs.values().filter(|j| j.state == "RUNNING").count()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_job_lifecycle() {
        let jm = JobManager::new();

        jm.submit_job("job-1".to_string(), "test-job".to_string(), 4, 1000);
        let job = jm.get_job("job-1").unwrap();
        assert_eq!(job.state, "CREATED");
        assert_eq!(job.parallelism, 4);

        jm.update_state("job-1", "RUNNING".to_string());
        assert_eq!(jm.get_job("job-1").unwrap().state, "RUNNING");
        assert_eq!(jm.running_count(), 1);

        jm.cancel_job("job-1");
        assert_eq!(jm.get_job("job-1").unwrap().state, "CANCELLED");
        assert_eq!(jm.running_count(), 0);
    }

    #[test]
    fn test_list_jobs() {
        let jm = JobManager::new();
        jm.submit_job("j1".to_string(), "job1".to_string(), 2, 1000);
        jm.submit_job("j2".to_string(), "job2".to_string(), 4, 2000);
        assert_eq!(jm.list_jobs().len(), 2);
    }
}
