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

//! File-backed raft log + vote storage.
//!
//! Layout under the raft dir:
//! - `log.jsonl`  — one JSON `Entry<Types>` per line (append + fsync)
//! - `vote.json`  — last persisted vote (atomic tmp+fsync+rename)
//! - `purged.json`— last purged log id (holes' lower bound)
//!
//! The control-plane write volume is tiny (one entry per job/task
//! transition), so a whole-file rewrite on truncate/purge is fine and
//! keeps the implementation auditable.

use std::collections::BTreeMap;
use std::io::Write;
use std::ops::RangeBounds;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use openraft::storage::RaftLogStorage;
use openraft::{Entry, LogId, LogState, RaftLogReader, StorageError, Vote};

use super::Types;

type SharedInner = Arc<Mutex<LogInner>>;

/// The store handed to openraft.
pub struct FileLogStore {
    inner: SharedInner,
}

/// Cloneable reader handed to replication tasks.
pub struct FileLogReader {
    inner: SharedInner,
}

struct LogInner {
    dir: PathBuf,
    /// In-memory copy of the on-disk log (always equal after each op).
    entries: BTreeMap<u64, Entry<Types>>,
    vote: Option<Vote<u64>>,
    last_purged: Option<LogId<u64>>,
}

impl FileLogStore {
    pub fn new(dir: &Path) -> anyhow::Result<Self> {
        let inner = LogInner {
            dir: dir.to_path_buf(),
            entries: BTreeMap::new(),
            vote: None,
            last_purged: None,
        };
        let store = FileLogStore {
            inner: Arc::new(Mutex::new(inner)),
        };
        store.load_from_disk()?;
        Ok(store)
    }

    fn load_from_disk(&self) -> anyhow::Result<()> {
        let mut inner = self.inner.lock().unwrap();
        if let Ok(bytes) = std::fs::read(inner.dir.join("vote.json")) {
            inner.vote = Some(serde_json::from_slice(&bytes)?);
        }
        if let Ok(bytes) = std::fs::read(inner.dir.join("purged.json")) {
            inner.last_purged = Some(serde_json::from_slice(&bytes)?);
        }
        if let Ok(bytes) = std::fs::read(inner.dir.join("log.jsonl")) {
            for line in bytes.split(|b| *b == b'\n') {
                if line.is_empty() {
                    continue;
                }
                let entry: Entry<Types> = serde_json::from_slice(line)?;
                inner.entries.insert(entry.log_id.index, entry);
            }
        }
        tracing::debug!(
            "raft log store: {} entr(y/ies), vote={:?}, purged={:?}",
            inner.entries.len(),
            inner.vote,
            inner.last_purged
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

fn write_atomic(path: &Path, bytes: &[u8]) -> std::io::Result<()> {
    let tmp = path.with_extension(format!("tmp.{}", std::process::id()));
    {
        let mut f = std::fs::File::create(&tmp)?;
        f.write_all(bytes)?;
        f.sync_all()?;
    }
    std::fs::rename(&tmp, path)?;
    if let Some(dir) = path.parent() {
        if let Ok(d) = std::fs::File::open(dir) {
            let _ = d.sync_all();
        }
    }
    Ok(())
}

impl LogInner {
    fn append_lines(&mut self, entries: impl Iterator<Item = Entry<Types>>) -> Result<(), StorageError<u64>> {
        let path = self.dir.join("log.jsonl");
        let mut file = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&path)
            .map_err(storage_err)?;
        for entry in entries {
            let line = serde_json::to_vec(&entry).map_err(storage_err)?;
            file.write_all(&line).map_err(storage_err)?;
            file.write_all(b"\n").map_err(storage_err)?;
            // Keep the memory copy in lockstep with the disk file.
            self.entries.insert(entry.log_id.index, entry);
        }
        file.sync_all().map_err(storage_err)?;
        Ok(())
    }

    fn rewrite_log(&mut self) -> Result<(), StorageError<u64>> {
        let path = self.dir.join("log.jsonl");
        let mut buf = Vec::new();
        for entry in self.entries.values() {
            let line = serde_json::to_vec(entry).map_err(storage_err)?;
            buf.extend_from_slice(&line);
            buf.push(b'\n');
        }
        write_atomic(&path, &buf).map_err(storage_err)
    }
}

impl RaftLogReader<Types> for FileLogReader {
    async fn try_get_log_entries<RB: RangeBounds<u64> + Clone + Send + std::fmt::Debug>(
        &mut self,
        range: RB,
    ) -> Result<Vec<Entry<Types>>, StorageError<u64>> {
        let inner = self.inner.lock().unwrap();
        Ok(inner
            .entries
            .range(range)
            .map(|(_, e)| e.clone())
            .collect())
    }
}

// The store itself must be a reader (RaftLogStorage supertrait).
impl RaftLogReader<Types> for FileLogStore {
    async fn try_get_log_entries<RB: RangeBounds<u64> + Clone + Send + std::fmt::Debug>(
        &mut self,
        range: RB,
    ) -> Result<Vec<Entry<Types>>, StorageError<u64>> {
        let inner = self.inner.lock().unwrap();
        Ok(inner
            .entries
            .range(range)
            .map(|(_, e)| e.clone())
            .collect())
    }
}

impl RaftLogStorage<Types> for FileLogStore {
    type LogReader = FileLogReader;

    async fn get_log_state(&mut self) -> Result<LogState<Types>, StorageError<u64>> {
        let inner = self.inner.lock().unwrap();
        Ok(LogState {
            last_purged_log_id: inner.last_purged,
            last_log_id: inner.entries.last_key_value().map(|(_, e)| e.log_id),
        })
    }

    async fn get_log_reader(&mut self) -> Self::LogReader {
        FileLogReader {
            inner: Arc::clone(&self.inner),
        }
    }

    async fn save_vote(&mut self, vote: &Vote<u64>) -> Result<(), StorageError<u64>> {
        let inner = self.inner.lock().unwrap();
        let bytes = serde_json::to_vec(vote).map_err(storage_err)?;
        write_atomic(&inner.dir.join("vote.json"), &bytes).map_err(storage_err)?;
        drop(inner);
        self.inner.lock().unwrap().vote = Some(*vote);
        Ok(())
    }

    async fn read_vote(&mut self) -> Result<Option<Vote<u64>>, StorageError<u64>> {
        Ok(self.inner.lock().unwrap().vote)
    }

    async fn append<I>(&mut self, entries: I, callback: openraft::storage::LogFlushed<Types>) -> Result<(), StorageError<u64>>
    where
        I: IntoIterator<Item = Entry<Types>> + Send,
        I::IntoIter: Send,
    {
        let collected: Vec<Entry<Types>> = entries.into_iter().collect();
        let res = {
            let mut inner = self.inner.lock().unwrap();
            inner.append_lines(collected.into_iter())
        };
        // openraft expects the callback regardless of ordering; call it
        // with the outcome once persistence completed.
        let io_result = match &res {
            Ok(()) => Ok(()),
            Err(e) => {
                let msg = match e {
                    StorageError::IO { source } => source.to_string(),
                    _ => e.to_string(),
                };
                Err(std::io::Error::other(msg))
            }
        };
        callback.log_io_completed(io_result);
        res
    }

    async fn truncate(&mut self, log_id: LogId<u64>) -> Result<(), StorageError<u64>> {
        let mut inner = self.inner.lock().unwrap();
        inner.entries.retain(|idx, _| *idx < log_id.index);
        inner.rewrite_log()
    }

    async fn purge(&mut self, log_id: LogId<u64>) -> Result<(), StorageError<u64>> {
        let mut inner = self.inner.lock().unwrap();
        inner.entries.retain(|idx, _| *idx > log_id.index);
        inner.last_purged = Some(log_id);
        inner.rewrite_log()?;
        let bytes = serde_json::to_vec(&log_id).map_err(storage_err)?;
        write_atomic(&inner.dir.join("purged.json"), &bytes).map_err(storage_err)
    }
}
