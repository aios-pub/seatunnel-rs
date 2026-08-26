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

use crate::row::Row;
use crate::schema::TableSchema;

/// Boxed dyn-compatible [`SinkWriter`] bound to a [`Sink`]'s associated types.
pub type BoxedSinkWriter<I, WS, CI> =
    Box<dyn SinkWriter<Input = I, WriterState = WS, CommitInfo = CI>>;

/// A sink connector that writes data from the pipeline.
///
/// Mirrors Java's `SeaTunnelSink<IN, StateT, CommitInfoT, AggregatedCommitInfoT>`.
/// Supports two-phase commit (2PC) for exactly-once semantics.
pub trait Sink: Send + Sync {
    /// The input data type.
    type Input: Into<Row>;

    /// Per-writer checkpoint state.
    type WriterState: serde::Serialize + for<'de> serde::Deserialize<'de> + Send + Sync;

    /// Per-writer commit information.
    type CommitInfo: serde::Serialize + for<'de> serde::Deserialize<'de> + Send + Sync;

    /// Aggregated commit info across all writers.
    type AggregatedCommitInfo: serde::Serialize + for<'de> serde::Deserialize<'de> + Send + Sync;

    /// Create a sink writer.
    fn create_writer(
        &self,
        writer_context: &SinkWriterContext,
    ) -> anyhow::Result<BoxedSinkWriter<Self::Input, Self::WriterState, Self::CommitInfo>>;

    /// Restore a writer from checkpoint state.
    fn restore_writer(
        &self,
        writer_context: &SinkWriterContext,
        states: &[Vec<u8>],
    ) -> anyhow::Result<BoxedSinkWriter<Self::Input, Self::WriterState, Self::CommitInfo>>;

    /// Get the schema of data this sink consumes.
    fn get_input_schema(&self) -> Option<TableSchema>;

    /// Create a committer for phase 2 of 2PC.
    fn create_committer(
        &self,
    ) -> Option<
        Box<
            dyn SinkCommitter<
                CommitInfo = Self::CommitInfo,
                AggregatedCommitInfo = Self::AggregatedCommitInfo,
            >,
        >,
    >;
}

/// Context for sink writer creation.
#[derive(Debug, Clone)]
pub struct SinkWriterContext {
    pub subtask: usize,
    pub parallelism: usize,
    pub job_id: String,
}

impl SinkWriterContext {
    pub fn new(subtask: usize, parallelism: usize, job_id: impl Into<String>) -> Self {
        SinkWriterContext {
            subtask,
            parallelism,
            job_id: job_id.into(),
        }
    }
}
use super::{sink_committer::SinkCommitter, sink_writer::SinkWriter};
