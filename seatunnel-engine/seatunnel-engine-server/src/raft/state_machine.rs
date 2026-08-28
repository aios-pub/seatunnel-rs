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

//! Raft state machine over the JobCoordinator: commands apply to the
//! coordinator, snapshots are the existing `export_state` JSON (one
//! format for snapshots, HA sync and operator inspection).
//!
//! Persistence model: persistent snapshot + log replay (apply() need not
//! fsync state; restart = last snapshot + committed log replay, both
//! durable in the log store).

use std::io::{Cursor, SeekFrom};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use openraft::storage::RaftStateMachine;
use openraft::{
    Entry, EntryPayload, LogId, RaftSnapshotBuilder, RaftTypeConfig, Snapshot, SnapshotMeta,
    StorageError, StoredMembership,
};
use tokio::io::AsyncReadExt;

use crate::job_coordinator::{CommandResult, JobCoordinator};

use super::Types;

/// The state machine handed to openraft. Cloning shares the coordinator.
pub struct CoordinatorStateMachine {
    pub coordinator: Arc<JobCoordinator>,
    inner: Arc<tokio::sync::Mutex<SmInner>>,
}

pub struct SmInner {
    dir: PathBuf,
    last_applied: Option<LogId<u64>>,
    membership: StoredMembership<u64, openraft::BasicNode>,
    /// Bytes of the last installed/built snapshot (for get_current_snapshot).
    current_snapshot: Option<Snapshot<Types>>,
}

impl CoordinatorStateMachine {
    pub async fn new(
        coordinator: Arc<JobCoordinator>,
        dir: &Path,
    ) -> anyhow::Result<Self> {
        let inner = SmInner {
            dir: dir.to_path_buf(),
            last_applied: None,
            membership: StoredMembership::default(),
            current_snapshot: None,
        };
        let sm = CoordinatorStateMachine {
            coordinator,
            inner: Arc::new(tokio::sync::Mutex::new(inner)),
        };
        sm.load_snapshot_from_disk().await?;
        Ok(sm)
    }

    /// Load `snapshot.json` (if any) at startup: replaces the in-memory
    /// coordinator state and restores the applied pointer.
    async fn load_snapshot_from_disk(&self) -> anyhow::Result<()> {
        let path = self.inner.lock().await.dir.join("snapshot.json");
        let Ok(bytes) = tokio::fs::read(&path).await else {
            return Ok(());
        };
        let Ok(value) = serde_json::from_slice::<serde_json::Value>(&bytes) else {
            tracing::warn!("raft snapshot unreadable; starting from log replay only");
            return Ok(());
        };
        let state = value["state"].clone();
        self.coordinator.replace_state(&state).await;
        let mut inner = self.inner.lock().await;
        inner.membership =
            serde_json::from_value(value["membership"].clone()).unwrap_or_default();
        inner.last_applied = serde_json::from_value(value["last_applied"].clone()).unwrap_or(None);
        inner.current_snapshot = None; // rebuilt lazily on demand
        tracing::info!(
            "raft snapshot loaded: last_applied={:?}",
            inner.last_applied
        );
        Ok(())
    }
}

fn storage_err<E: std::fmt::Display>(e: E) -> StorageError<u64> {
    StorageError::from_io_error(
        openraft::ErrorSubject::Store,
        openraft::ErrorVerb::Write,
        std::io::Error::other(e.to_string()),
    )
}

/// Tiny-file atomic write: tmp + fsync + rename + dir fsync (std is fine
/// at snapshot sizes; called from async context sparingly).
fn write_synced(tmp: &Path, final_path: &Path, bytes: &[u8]) -> std::io::Result<()> {
    use std::io::Write;
    let mut f = std::fs::File::create(tmp)?;
    f.write_all(bytes)?;
    f.sync_all()?;
    std::fs::rename(tmp, final_path)?;
    if let Some(dir) = final_path.parent() {
        if let Ok(d) = std::fs::File::open(dir) {
            let _ = d.sync_all();
        }
    }
    Ok(())
}

#[allow(refining_impl_trait)]
impl RaftStateMachine<Types> for CoordinatorStateMachine {
    type SnapshotBuilder = SmSnapshotBuilder;

    async fn applied_state(
        &mut self,
    ) -> Result<(Option<LogId<u64>>, StoredMembership<u64, openraft::BasicNode>), StorageError<u64>> {
        let inner = self.inner.lock().await;
        Ok((inner.last_applied, inner.membership.clone()))
    }

    async fn apply<I>(&mut self, entries: I) -> Result<Vec<CommandResult>, StorageError<u64>>
    where
        I: IntoIterator<Item = Entry<Types>> + Send,
        I::IntoIter: Send,
    {
        let mut results = Vec::new();
        for entry in entries {
            let result = match entry.payload {
                EntryPayload::Blank => CommandResult::Ok,
                EntryPayload::Membership(ref mem) => {
                    let mut inner = self.inner.lock().await;
                    inner.membership = StoredMembership::new(Some(entry.log_id), mem.clone());
                    CommandResult::Ok
                }
                EntryPayload::Normal(ref cmd) => self.coordinator.apply_command(cmd),
            };
            let mut inner = self.inner.lock().await;
            inner.last_applied = Some(entry.log_id);
            drop(inner);
            results.push(result);
        }
        Ok(results)
    }

    async fn get_snapshot_builder(&mut self) -> Self::SnapshotBuilder {
        SmSnapshotBuilder {
            coordinator: Arc::clone(&self.coordinator),
            inner: Arc::clone(&self.inner),
        }
    }

    async fn begin_receiving_snapshot(&mut self) -> Result<Box<<Types as RaftTypeConfig>::SnapshotData>, StorageError<u64>> {
        Ok(Box::new(Cursor::new(Vec::new())))
    }

    async fn install_snapshot(
        &mut self,
        meta: &SnapshotMeta<u64, openraft::BasicNode>,
        snapshot: Box<<Types as RaftTypeConfig>::SnapshotData>,
    ) -> Result<(), StorageError<u64>> {
        let mut cursor = snapshot;
        let mut bytes = Vec::new();
        tokio::io::AsyncSeekExt::seek(&mut cursor, SeekFrom::Start(0))
            .await
            .map_err(storage_err)?;
        cursor
            .read_to_end(&mut bytes)
            .await
            .map_err(storage_err)?;
        let value: serde_json::Value = serde_json::from_slice(&bytes).map_err(storage_err)?;
        let state = value["state"].clone();

        // Replace local state with the leader's authoritative snapshot.
        self.coordinator.replace_state(&state).await;
        {
            let mut inner = self.inner.lock().await;
            inner.membership = StoredMembership::new(
                meta.last_log_id,
                meta.last_membership.membership().clone(),
            );
            inner.last_applied = meta.last_log_id;
            // Persist: snapshot.json holds {state, membership, applied}.
            let doc = serde_json::json!({
                "state": state,
                "membership": serde_json::to_value(&inner.membership).map_err(storage_err)?,
            });
            let path = inner.dir.join("snapshot.json");
            let tmp = inner.dir.join("snapshot.json.tmp");
            let bytes = serde_json::to_vec(&doc).map_err(storage_err)?;
            write_synced(&tmp, &path, &bytes).map_err(storage_err)?;
            inner.current_snapshot = None;
        }
        tracing::info!(
            "raft snapshot installed: last_applied={:?}",
            meta.last_log_id
        );
        Ok(())
    }

    async fn get_current_snapshot(&mut self) -> Result<Option<Snapshot<Types>>, StorageError<u64>> {
        let mut inner = self.inner.lock().await;
        if inner.current_snapshot.is_none() {
            // Rebuild from disk if a snapshot exists (post-restart).
            let path = inner.dir.join("snapshot.json");
            if let Ok(bytes) = tokio::fs::read(&path).await {
                let value: serde_json::Value =
                    serde_json::from_slice(&bytes).map_err(storage_err)?;
                let meta: SnapshotMeta<u64, openraft::BasicNode> =
                    serde_json::from_value(value["meta"].clone()).map_err(storage_err)?;
                inner.current_snapshot = Some(Snapshot {
                    meta,
                    snapshot: Box::new(Cursor::new(bytes)),
                });
            }
        }
        Ok(inner.current_snapshot.clone())
    }
}

impl Clone for CoordinatorStateMachine {
    fn clone(&self) -> Self {
        Self {
            coordinator: Arc::clone(&self.coordinator),
            inner: Arc::clone(&self.inner),
        }
    }
}

/// Builds a snapshot from the live coordinator state.
pub struct SmSnapshotBuilder {
    coordinator: Arc<JobCoordinator>,
    inner: Arc<tokio::sync::Mutex<SmInner>>,
}

impl RaftSnapshotBuilder<Types> for SmSnapshotBuilder {
    async fn build_snapshot(&mut self) -> Result<Snapshot<Types>, StorageError<u64>> {
        let state = self.coordinator.export_state().await;
        let mut inner = self.inner.lock().await;
        let meta = SnapshotMeta {
            last_log_id: inner.last_applied,
            last_membership: inner.membership.clone(),
            snapshot_id: format!(
                "snapshot-{}",
                inner.last_applied.map(|l| l.index).unwrap_or(0)
            ),
        };
        let doc = serde_json::json!({
            "state": state,
            "membership": serde_json::to_value(&inner.membership).map_err(storage_err)?,
            "last_applied": serde_json::to_value(&meta.last_log_id).map_err(storage_err)?,
            "meta": serde_json::to_value(&meta).map_err(storage_err)?,
        });
        let bytes = serde_json::to_vec(&doc).map_err(storage_err)?;
        // Persist the built snapshot immediately (persistent-snapshot model).
        let path = inner.dir.join("snapshot.json");
        let tmp = inner.dir.join("snapshot.json.tmp");
        write_synced(&tmp, &path, &bytes).map_err(storage_err)?;

        inner.current_snapshot = None; // rebuilt with fresh bytes on demand
        Ok(Snapshot {
            meta,
            snapshot: Box::new(Cursor::new(bytes)),
        })
    }
}
