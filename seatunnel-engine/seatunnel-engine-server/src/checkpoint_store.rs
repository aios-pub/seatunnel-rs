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

//! Shared checkpoint stores: master-backed (memory + local disk + HA
//! replication) and S3-compatible (object_store; MinIO/AWS).
//!
//! Layout (both backends): `<root>/<job_id>/<task_id>/cp-<id>.state`.
//! Writes are best-effort: a store outage logs an ERROR and skips the
//! checkpoint round — the data plane keeps running on worker-local state.

use std::collections::HashMap;
use std::sync::Arc;

use object_store::ObjectStore;
use object_store::path::Path;
use tokio::sync::RwLock;

use crate::server_config::ResolvedS3Config;

// ---------------------------------------------------------------------------
// Master-backed store (checkpoint.storage.type = master)
// ---------------------------------------------------------------------------

/// One task's checkpoint history (newest last).
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, Default)]
pub struct TaskCheckpoints {
    pub entries: Vec<(u64, Vec<u8>)>,
}

/// Metadata of a single retained checkpoint (id + size, no payload).
#[derive(Debug, Clone)]
pub struct CheckpointMetaEntry {
    pub checkpoint_id: u64,
    pub size_bytes: u64,
}

/// Checkpoint metadata for one task, for read-only listings (web UI).
#[derive(Debug, Clone)]
pub struct TaskCheckpointMeta {
    pub task_id: String,
    pub entries: Vec<CheckpointMetaEntry>,
}

/// In-memory master store with bounded per-task retention; replicated to
/// standby masters via the coordinator state snapshot.
#[derive(Debug, Default)]
pub struct MasterCheckpointStore {
    // (job_id, task_id) → checkpoints ordered by id.
    inner: RwLock<HashMap<(String, String), TaskCheckpoints>>,
    retained: usize,
}

impl MasterCheckpointStore {
    pub fn new(retained: usize) -> Self {
        MasterCheckpointStore {
            inner: RwLock::new(HashMap::new()),
            retained: retained.max(1),
        }
    }

    pub async fn save(&self, job_id: &str, task_id: &str, checkpoint_id: u64, data: &[u8]) {
        let mut inner = self.inner.write().await;
        let task = inner
            .entry((job_id.to_string(), task_id.to_string()))
            .or_default();
        task.entries.retain(|(id, _)| *id != checkpoint_id);
        task.entries.push((checkpoint_id, data.to_vec()));
        task.entries.sort_by_key(|(id, _)| *id);
        let excess = task.entries.len().saturating_sub(self.retained);
        if excess > 0 {
            task.entries.drain(..excess);
        }
    }

    pub async fn load_latest(&self, job_id: &str, task_id: &str) -> Option<(u64, Vec<u8>)> {
        let inner = self.inner.read().await;
        inner
            .get(&(job_id.to_string(), task_id.to_string()))
            .and_then(|t| t.entries.last().cloned())
    }

    /// List retained checkpoint metadata for every task of a job, sorted by
    /// task id. Does not copy payload bytes.
    pub async fn list_job_meta(&self, job_id: &str) -> Vec<TaskCheckpointMeta> {
        let inner = self.inner.read().await;
        let mut out: Vec<TaskCheckpointMeta> = inner
            .iter()
            .filter(|((job, _), _)| job == job_id)
            .map(|((_, task_id), t)| TaskCheckpointMeta {
                task_id: task_id.clone(),
                entries: t
                    .entries
                    .iter()
                    .map(|(id, data)| CheckpointMetaEntry {
                        checkpoint_id: *id,
                        size_bytes: data.len() as u64,
                    })
                    .collect(),
            })
            .collect();
        out.sort_by(|a, b| a.task_id.cmp(&b.task_id));
        out
    }

    pub async fn drop_job(&self, job_id: &str) {
        self.inner.write().await.retain(|(job, _), _| job != job_id);
    }

    /// Full snapshot for HA replication (JSON-safe entry list).
    pub async fn export(&self) -> Vec<((String, String), TaskCheckpoints)> {
        self.inner
            .read()
            .await
            .iter()
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect()
    }

    /// Merge a replication snapshot (imported entries only fill gaps —
    /// local state wins, matching the coordinator import policy).
    pub async fn import(&self, snapshot: Vec<((String, String), TaskCheckpoints)>) {
        let mut inner = self.inner.write().await;
        for (key, task) in snapshot {
            inner.entry(key).or_insert(task);
        }
    }
}

// ---------------------------------------------------------------------------
// S3-compatible store (checkpoint.storage.type = s3)
// ---------------------------------------------------------------------------

/// Build an object_store client from resolved config (S3/MinIO).
pub fn build_object_store(config: &ResolvedS3Config) -> anyhow::Result<Arc<dyn ObjectStore>> {
    let mut builder = object_store::aws::AmazonS3Builder::new().with_bucket_name(&config.bucket);
    if !config.endpoint.is_empty() {
        builder = builder.with_endpoint(&config.endpoint);
    }
    if !config.region.is_empty() {
        builder = builder.with_region(&config.region);
    }
    if let Some(key) = &config.access_key {
        builder = builder.with_access_key_id(key);
    }
    if let Some(secret) = &config.secret_key {
        builder = builder.with_secret_access_key(secret);
    }
    if config.path_style {
        // MinIO: bucket in the path, not the hostname.
        builder = builder.with_virtual_hosted_style_request(false);
    }
    if config.endpoint.starts_with("http://") {
        // Plain-HTTP endpoints (MinIO) must be explicitly allowed.
        builder = builder.with_allow_http(true);
    }
    let store = builder
        .build()
        .map_err(|e| anyhow::anyhow!("S3 checkpoint store config: {}", e))?;
    Ok(Arc::new(store))
}

/// S3-backed checkpoint store: workers write directly (Java external-storage
/// model); the master only sweeps.
#[derive(Clone)]
pub struct S3CheckpointStore {
    store: Arc<dyn ObjectStore>,
    prefix: String,
    retained: usize,
}

impl S3CheckpointStore {
    pub fn new(store: Arc<dyn ObjectStore>, prefix: &str, retained: usize) -> Self {
        S3CheckpointStore {
            store,
            prefix: prefix.trim_matches('/').to_string(),
            retained: retained.max(1),
        }
    }

    fn object_path(&self, job_id: &str, task_id: &str, checkpoint_id: u64) -> Path {
        Path::from(format!(
            "{}/{}/{}/cp-{}.state",
            self.prefix, job_id, task_id, checkpoint_id
        ))
    }

    fn task_prefix(&self, job_id: &str, task_id: &str) -> Path {
        Path::from(format!("{}/{}/{}", self.prefix, job_id, task_id))
    }

    fn job_prefix(&self, job_id: &str) -> Path {
        Path::from(format!("{}/{}", self.prefix, job_id))
    }

    /// Write one checkpoint; prune older objects beyond `retained`.
    /// Storage failures are logged and skipped (data plane continues on
    /// worker-local state).
    pub async fn save(&self, job_id: &str, task_id: &str, checkpoint_id: u64, data: &[u8]) {
        let path = self.object_path(job_id, task_id, checkpoint_id);
        if let Err(e) = self.store.put(&path, data.to_vec().into()).await {
            tracing::error!(
                "S3 checkpoint put {} failed (skipping this round): {}",
                path,
                e
            );
            return;
        }
        if let Err(e) = self.prune_task(job_id, task_id).await {
            tracing::warn!("S3 checkpoint prune for task {} failed: {}", task_id, e);
        }
    }

    /// Newest checkpoint for a task (for the worker restore chain).
    pub async fn load_latest(&self, job_id: &str, task_id: &str) -> Option<(u64, Vec<u8>)> {
        let prefix = self.task_prefix(job_id, task_id);
        let best = self.list_checkpoints(&prefix).await.ok()?;
        let (id, path) = best?;
        match self.store.get(&path).await {
            Ok(result) => match result.bytes().await {
                Ok(bytes) => Some((id, bytes.to_vec())),
                Err(e) => {
                    tracing::error!("S3 checkpoint get {} failed: {}", path, e);
                    None
                }
            },
            Err(e) => {
                tracing::error!("S3 checkpoint get {} failed: {}", path, e);
                None
            }
        }
    }

    /// Delete every object under a job (cancel/completion cleanup).
    pub async fn drop_job(&self, job_id: &str) {
        let prefix = self.job_prefix(job_id);
        let objects: Vec<Path> = match self.list_all(&prefix).await {
            Ok(paths) => paths,
            Err(e) => {
                tracing::warn!("S3 drop_job {} listing failed: {}", job_id, e);
                return;
            }
        };
        let removed = objects.len();
        for path in objects {
            if let Err(e) = self.store.delete(&path).await {
                tracing::warn!("S3 delete {} failed: {}", path, e);
            }
        }
        tracing::info!(
            "S3 cleanup: removed {} object(s) for job {}",
            removed,
            job_id
        );
    }

    /// TTL sweep: remove job prefixes whose newest object is older than
    /// `ttl` (Java history-job-expire-minutes). Returns swept job ids.
    pub async fn sweep_expired(&self, ttl: std::time::Duration) -> Vec<String> {
        let root = Path::from(self.prefix.clone());
        let jobs = match self.list_distinct_first_segments(&root).await {
            Ok(jobs) => jobs,
            Err(e) => {
                tracing::debug!("S3 sweep listing failed: {}", e);
                return Vec::new();
            }
        };
        let mut swept = Vec::new();
        for job in jobs {
            let job_prefix = self.job_prefix(&job);
            let paths = match self.list_all_with_last_modified(&job_prefix).await {
                Ok(p) => p,
                Err(_) => continue,
            };
            if paths.is_empty() {
                continue;
            }
            let newest = paths
                .iter()
                .map(|(_, lm)| *lm)
                .max()
                .unwrap_or(std::time::UNIX_EPOCH);
            let age = std::time::SystemTime::now()
                .duration_since(newest)
                .unwrap_or_default();
            if age >= ttl {
                for (path, _) in paths {
                    let _ = self.store.delete(&path).await;
                }
                tracing::info!(
                    "S3 sweep: removed expired job '{}' (idle {:.1}h)",
                    job,
                    age.as_secs_f64() / 3600.0
                );
                swept.push(job);
            }
        }
        swept
    }

    async fn list_checkpoints(&self, prefix: &Path) -> anyhow::Result<Option<(u64, Path)>> {
        let mut best: Option<(u64, Path)> = None;
        let mut listing = self.store.list(Some(prefix));
        use futures::StreamExt;
        while let Some(item) = listing.next().await {
            let meta = item.map_err(|e| anyhow::anyhow!("{}", e))?;
            let name = meta.location.filename().unwrap_or_default().to_string();
            let Some(rest) = name
                .strip_prefix("cp-")
                .and_then(|r| r.strip_suffix(".state"))
            else {
                continue;
            };
            let Ok(id) = rest.parse::<u64>() else {
                continue;
            };
            if best.as_ref().map(|(b, _)| id > *b).unwrap_or(true) {
                best = Some((id, meta.location.clone()));
            }
        }
        Ok(best)
    }

    async fn list_all(&self, prefix: &Path) -> anyhow::Result<Vec<Path>> {
        let mut out = Vec::new();
        let mut listing = self.store.list(Some(prefix));
        use futures::StreamExt;
        while let Some(item) = listing.next().await {
            let meta = item.map_err(|e| anyhow::anyhow!("{}", e))?;
            out.push(meta.location.clone());
        }
        Ok(out)
    }

    async fn list_all_with_last_modified(
        &self,
        prefix: &Path,
    ) -> anyhow::Result<Vec<(Path, std::time::SystemTime)>> {
        let mut out = Vec::new();
        let mut listing = self.store.list(Some(prefix));
        use futures::StreamExt;
        while let Some(item) = listing.next().await {
            let meta = item.map_err(|e| anyhow::anyhow!("{}", e))?;
            out.push((meta.location.clone(), meta.last_modified.into()));
        }
        Ok(out)
    }

    /// Job ids = distinct first path segments under the root prefix.
    async fn list_distinct_first_segments(&self, root: &Path) -> anyhow::Result<Vec<String>> {
        let mut jobs = Vec::new();
        let mut listing = self.store.list(Some(root));
        use futures::StreamExt;
        while let Some(item) = listing.next().await {
            let meta = item.map_err(|e| anyhow::anyhow!("{}", e))?;
            // Path after root, first segment.
            let rel = meta
                .location
                .as_ref()
                .strip_prefix(&format!("{}/", root.as_ref()))
                .unwrap_or(meta.location.as_ref());
            if let Some(job) = rel.split('/').next() {
                if !job.is_empty() && !jobs.iter().any(|j: &String| j == job) {
                    jobs.push(job.to_string());
                }
            }
        }
        Ok(jobs)
    }

    /// Keep only the newest `retained` checkpoints of a task.
    async fn prune_task(&self, job_id: &str, task_id: &str) -> anyhow::Result<()> {
        let prefix = self.task_prefix(job_id, task_id);
        let mut ids: Vec<(u64, Path)> = Vec::new();
        let mut listing = self.store.list(Some(&prefix));
        use futures::StreamExt;
        while let Some(item) = listing.next().await {
            let meta = item.map_err(|e| anyhow::anyhow!("{}", e))?;
            let name = meta.location.filename().unwrap_or_default().to_string();
            let Some(rest) = name
                .strip_prefix("cp-")
                .and_then(|r| r.strip_suffix(".state"))
            else {
                continue;
            };
            if let Ok(id) = rest.parse::<u64>() {
                ids.push((id, meta.location.clone()));
            }
        }
        if ids.len() <= self.retained {
            return Ok(());
        }
        ids.sort_by_key(|(id, _)| *id);
        let excess = ids.len() - self.retained;
        for (_, path) in &ids[..excess] {
            let _ = self.store.delete(path).await;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn mem_store(retained: usize) -> S3CheckpointStore {
        S3CheckpointStore::new(
            Arc::new(object_store::memory::InMemory::new()),
            "checkpoints",
            retained,
        )
    }

    #[tokio::test]
    async fn master_store_roundtrip_and_retention() {
        let store = MasterCheckpointStore::new(2);
        store.save("j", "t", 1, b"one").await;
        store.save("j", "t", 2, b"two").await;
        store.save("j", "t", 3, b"three").await;
        let (id, data) = store.load_latest("j", "t").await.unwrap();
        assert_eq!((id, data.as_slice()), (3, b"three".as_slice()));
        // Retention kept 2.
        let exported = store.export().await;
        let entry = exported
            .iter()
            .find(|((job, task), _)| job == "j" && task == "t")
            .expect("task exported");
        assert_eq!(entry.1.entries.len(), 2);
        store.drop_job("j").await;
        assert!(store.load_latest("j", "t").await.is_none());
    }

    #[tokio::test]
    async fn s3_save_load_latest() {
        let store = mem_store(3);
        store.save("job1", "task1", 1, b"state-one").await;
        store.save("job1", "task1", 2, b"state-two").await;
        let (id, data) = store.load_latest("job1", "task1").await.unwrap();
        assert_eq!(id, 2);
        assert_eq!(data, b"state-two");
    }

    #[tokio::test]
    async fn s3_write_prunes_old_objects() {
        let store = mem_store(2);
        for id in 1..=5u64 {
            store
                .save("j", "t", id, format!("s{}", id).as_bytes())
                .await;
        }
        // Only cp-4 and cp-5 remain.
        let (id, _) = store.load_latest("j", "t").await.unwrap();
        assert_eq!(id, 5);
        let paths = store.list_all(&store.task_prefix("j", "t")).await.unwrap();
        assert_eq!(paths.len(), 2);
    }

    #[tokio::test]
    async fn s3_drop_job_removes_everything() {
        let store = mem_store(3);
        store.save("j1", "t1", 1, b"x").await;
        store.save("j1", "t2", 1, b"y").await;
        store.save("j2", "t1", 1, b"z").await;
        store.drop_job("j1").await;
        assert!(store.load_latest("j1", "t1").await.is_none());
        assert!(store.load_latest("j1", "t2").await.is_none());
        assert!(store.load_latest("j2", "t1").await.is_some());
    }

    #[tokio::test]
    async fn s3_sweep_removes_expired_jobs() {
        let store = mem_store(3);
        store.save("old", "t", 1, b"x").await;
        store.save("fresh", "t", 1, b"y").await;
        // InMemoryStore has no controllable mtime; ttl 0 expires everything.
        let swept = store.sweep_expired(std::time::Duration::ZERO).await;
        assert!(swept.contains(&"old".to_string()));
        assert!(swept.contains(&"fresh".to_string()));
        assert!(store.load_latest("old", "t").await.is_none());
    }

    #[tokio::test]
    async fn s3_sweep_keeps_young_objects() {
        let store = mem_store(3);
        store.save("fresh", "t", 1, b"y").await;
        let swept = store
            .sweep_expired(std::time::Duration::from_secs(3600))
            .await;
        assert!(swept.is_empty());
        assert!(store.load_latest("fresh", "t").await.is_some());
    }
}
