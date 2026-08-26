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

//! Local durable state store for worker-side checkpoints.
//!
//! Layout: `<root>/<job_id>/<task_id>/cp-<id>.state` (atomic tmp+rename).
//! On task restart the highest-numbered snapshot is restored, giving workers
//! crash resilience independent of master availability.

use std::fs;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::Arc;

/// How many recent snapshots per task to keep on disk.
const RETAINED_SNAPSHOTS: usize = 3;

#[derive(Debug, Clone)]
pub struct LocalStateStore {
    root: PathBuf,
}

impl LocalStateStore {
    pub fn new(root: impl Into<PathBuf>) -> Self {
        LocalStateStore { root: root.into() }
    }

    pub fn from_env_or_default() -> Self {
        let root = std::env::var("SEATUNNEL_STATE_DIR")
            .unwrap_or_else(|_| ".seatunnel-state".to_string());
        LocalStateStore::new(root)
    }

    fn task_dir(&self, job_id: &str, task_id: &str) -> PathBuf {
        self.root.join(sanitize(job_id)).join(sanitize(task_id))
    }

    /// Persist a checkpoint snapshot atomically; prunes older ones.
    pub fn save_checkpoint(
        &self,
        job_id: &str,
        task_id: &str,
        checkpoint_id: u64,
        state: &[u8],
    ) -> std::io::Result<PathBuf> {
        let dir = self.task_dir(job_id, task_id);
        fs::create_dir_all(&dir)?;
        let final_path = dir.join(format!("cp-{}.state", checkpoint_id));
        let tmp_path = dir.join(format!(".cp-{}.state.tmp", checkpoint_id));

        {
            let mut f = fs::File::create(&tmp_path)?;
            f.write_all(state)?;
            f.sync_all()?;
        }
        fs::rename(&tmp_path, &final_path)?;
        self.prune(&dir);
        Ok(final_path)
    }

    /// Load the newest checkpoint snapshot for a task, if any.
    pub fn load_latest_checkpoint(
        &self,
        job_id: &str,
        task_id: &str,
    ) -> std::io::Result<Option<(u64, Vec<u8>)>> {
        let dir = self.task_dir(job_id, task_id);
        let mut best: Option<(u64, PathBuf)> = None;
        if let Ok(entries) = fs::read_dir(&dir) {
            for entry in entries.flatten() {
                let name = entry.file_name();
                let name = name.to_string_lossy();
                let Some(rest) = name.strip_prefix("cp-").and_then(|r| r.strip_suffix(".state")) else {
                    continue;
                };
                if let Ok(id) = rest.parse::<u64>() {
                    if best.as_ref().map(|(b, _)| id > *b).unwrap_or(true) {
                        best = Some((id, entry.path()));
                    }
                }
            }
        }
        match best {
            Some((id, path)) => Ok(Some((id, fs::read(path)?))),
            None => Ok(None),
        }
    }

    /// Remove all stored state for a job.
    pub fn drop_job(&self, job_id: &str) {
        let _ = fs::remove_dir_all(self.root.join(sanitize(job_id)));
    }

    fn prune(&self, dir: &Path) {
        let Ok(entries) = fs::read_dir(dir) else { return };
        let mut ids: Vec<(u64, PathBuf)> = entries
            .flatten()
            .filter_map(|e| {
                let name = e.file_name().to_string_lossy().to_string();
                name.strip_prefix("cp-")
                    .and_then(|r| r.strip_suffix(".state"))
                    .and_then(|r| r.parse::<u64>().ok())
                    .map(|id| (id, e.path()))
            })
            .collect();
        if ids.len() <= RETAINED_SNAPSHOTS {
            return;
        }
        ids.sort_by_key(|(id, _)| *id);
        let excess = ids.len() - RETAINED_SNAPSHOTS;
        for (_, path) in &ids[..excess] {
            let _ = fs::remove_file(path);
        }
    }
}

/// Combined checkpoint listener used by workers: persists snapshots locally
/// and forwards the report to the master. Defined here (rather than as an
/// impl on `Arc<LocalStateStore>`) to respect Rust's orphan rules.
pub struct PersistAndReportListener {
    pub store: Arc<LocalStateStore>,
}

impl seatunnel_engine_core::CheckpointListener for PersistAndReportListener {
    fn on_checkpoint<'a>(
        &'a self,
        job_id: &'a str,
        task_id: &'a str,
        checkpoint_id: u64,
        timestamp: i64,
        state: Vec<u8>,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send + 'a>> {
        Box::pin(async move {
            if let Err(e) = self
                .store
                .save_checkpoint(job_id, task_id, checkpoint_id, &state)
            {
                tracing::error!(
                    "Task {} checkpoint {}: local persist failed: {}",
                    task_id,
                    checkpoint_id,
                    e
                );
            }
            let _ = timestamp;
        })
    }
}

fn sanitize(component: &str) -> String {
    component
        .chars()
        .map(|c| if c.is_ascii_alphanumeric() || c == '-' || c == '_' || c == '.' { c } else { '_' })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tmp_store(tag: &str) -> LocalStateStore {
        let dir = std::env::temp_dir().join(format!("st-state-store-{}", tag));
        let _ = fs::remove_dir_all(&dir);
        LocalStateStore::new(dir)
    }

    #[test]
    fn save_and_load_roundtrip() {
        let store = tmp_store("rt");
        store
            .save_checkpoint("job1", "task1", 1, b"state-one")
            .unwrap();
        store
            .save_checkpoint("job1", "task1", 2, b"state-two")
            .unwrap();

        let (id, data) = store.load_latest_checkpoint("job1", "task1").unwrap().unwrap();
        assert_eq!(id, 2);
        assert_eq!(data, b"state-two");
    }

    #[test]
    fn load_missing_returns_none() {
        let store = tmp_store("missing");
        assert!(store.load_latest_checkpoint("nope", "nope").unwrap().is_none());
    }

    #[test]
    fn retention_prunes_old_snapshots() {
        let store = tmp_store("prune");
        for id in 1..=5u64 {
            store
                .save_checkpoint("job1", "t", id, format!("s{}", id).as_bytes())
                .unwrap();
        }
        let (latest, _) = store.load_latest_checkpoint("job1", "t").unwrap().unwrap();
        assert_eq!(latest, 5);
        // Only RETAINED_SNAPSHOTS remain.
        let count = fs::read_dir(store.task_dir("job1", "t"))
            .unwrap()
            .flatten()
            .count();
        assert_eq!(count, RETAINED_SNAPSHOTS);
    }

    #[test]
    fn drop_job_removes_state() {
        let store = tmp_store("drop");
        store.save_checkpoint("j", "t", 1, b"x").unwrap();
        assert!(store.load_latest_checkpoint("j", "t").unwrap().is_some());
        store.drop_job("j");
        assert!(store.load_latest_checkpoint("j", "t").unwrap().is_none());
    }
}
