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
use super::source_split_enum::SourceSplitEnumeratorContext;
use crate::schema::TableSchema;
use crate::source::source_reader::SourceReaderContext;

/// A source connector that produces data into the pipeline.
///
/// Mirrors Java's `SeaTunnelSource<T, SplitT, StateT>`.
///
/// # Lifecycle
/// 1. `create_enumagurator()` — called on the master node to discover splits
/// 2. `create_reader()` — called on worker nodes to read data from splits
/// 3. `restore_reader()` — called on recovery to restore reader state
pub trait Source: Send + Sync {
    /// The type of data this source produces.
    type Output: Into<crate::row::Row>;

    /// The type of split this source uses for parallelism.
    type Split: SourceSplit;

    /// The checkpoint state type for this source.
    type State: serde::Serialize + for<'de> serde::Deserialize<'de> + Send + Sync;

    /// Enumerate splits for parallel reading.
    /// Called once during job initialization on the master node.
    fn enumerate_splits(
        &self,
        context: &SourceSplitEnumeratorContext<Self::Split>,
    ) -> anyhow::Result<Vec<Self::Split>>;

    /// Create a reader for reading data from splits.
    /// Called on worker nodes.
    fn create_reader(
        &self,
        context: SourceReaderContext,
    ) -> anyhow::Result<
        Box<
            dyn crate::source::source_reader::SourceReader<
                Output = Self::Output,
                Split = Self::Split,
            >,
        >,
    >;

    /// Restore a reader from checkpoint state.
    fn restore_reader(
        &self,
        context: SourceReaderContext,
        state: &Self::State,
    ) -> anyhow::Result<
        Box<
            dyn crate::source::source_reader::SourceReader<
                Output = Self::Output,
                Split = Self::Split,
            >,
        >,
    >;

    /// Get the schema of data produced by this source.
    fn get_output_schema(&self) -> Option<TableSchema>;

    /// Get the boundedness of this source.
    fn boundedness(&self) -> Boundedness;
}

/// Whether a source produces bounded (batch) or unbounded (streaming) data.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Boundedness {
    /// Finite data set (batch mode)
    Bounded,
    /// Infinite data stream (streaming mode)
    Unbounded,
}

impl std::fmt::Display for Boundedness {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Boundedness::Bounded => write!(f, "BOUNDED"),
            Boundedness::Unbounded => write!(f, "UNBOUNDED"),
        }
    }
}
