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
use crate::row::Row;
use std::future::Future;
use std::pin::Pin;

#[derive(Debug, Clone)]
pub struct SourceReaderContext {
    pub subtask: usize,
    pub parallelism: usize,
    pub job_id: String,
}

/// Boxed future returned by async [`SourceReader`] operations.
pub type ReaderFuture<'a, T> = Pin<Box<dyn Future<Output = anyhow::Result<T>> + Send + 'a>>;

/// A source reader that produces data from splits.
pub trait SourceReader: Send {
    type Output: Into<Row> + Send;
    type Split: SourceSplit;

    fn open(&mut self) -> ReaderFuture<'_, ()>;
    fn poll_next(&mut self) -> ReaderFuture<'_, PollResult<Self::Output>>;
    fn snapshot_state(&mut self) -> ReaderFuture<'_, Vec<u8>>;
    fn add_splits(&mut self, splits: Vec<Self::Split>);
    fn handle_no_more_splits(&mut self);
    fn close(&mut self) -> ReaderFuture<'_, ()>;
}

#[derive(Debug)]
pub enum PollResult<T> {
    Record(T),
    Empty,
    EOF,
}
