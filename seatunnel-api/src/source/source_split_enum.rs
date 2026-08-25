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

use super::source_split::SourceSplit;

/// Context passed to the split enumerator.
#[derive(Debug, Clone)]
pub struct SourceSplitEnumeratorContext<Split: SourceSplit> {
    pub parallelism: usize,
    pub job_id: String,
    pub _marker: std::marker::PhantomData<Split>,
}

impl<Split: SourceSplit> SourceSplitEnumeratorContext<Split> {
    pub fn new(parallelism: usize, job_id: impl Into<String>) -> Self {
        SourceSplitEnumeratorContext {
            parallelism,
            job_id: job_id.into(),
            _marker: std::marker::PhantomData,
        }
    }
}

/// A split enumerator that assigns splits to readers.
///
/// Mirrors Java's `SourceSplitEnumerator<SplitT, StateT>`. Runs on the master node.
///
/// # Lifecycle
/// 1. `open()` — initialize
/// 2. `run()` — main loop: assign splits, handle requests
/// 3. `snapshot_state()` — checkpoint state
/// 4. `close()` — cleanup
pub trait SourceSplitEnumerator: Send {
    /// The split type.
    type Split: SourceSplit;

    /// The checkpoint state type.
    type State: serde::Serialize + for<'de> serde::Deserialize<'de>;

    /// Open the enumerator.
    async fn open(&mut self) -> anyhow::Result<()>;

    /// Register a new reader subtask.
    fn register_reader(&mut self, reader_id: usize);

    /// Handle a split request from a reader.
    fn handle_split_request(&mut self, reader_id: usize);

    /// Snapshot state for checkpointing.
    async fn snapshot_state(&mut self) -> anyhow::Result<Vec<u8>>;

    /// Restore state from checkpoint.
    fn restore_state(&mut self, state: &[u8]) -> anyhow::Result<()>;

    /// Close the enumerator.
    async fn close(&mut self);
}
