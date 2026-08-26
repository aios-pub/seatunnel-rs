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
use std::future::Future;
use std::pin::Pin;

/// Boxed future returned by async [`SinkWriter`] operations.
pub type WriterFuture<'a, T> = Pin<Box<dyn Future<Output = anyhow::Result<T>> + Send + 'a>>;

pub trait SinkWriter: Send {
    type Input: Into<Row> + Send;
    type WriterState: serde::Serialize + Send + Sync;
    type CommitInfo: serde::Serialize + Send + Sync;

    /// Open the writer and initialize any lazily-created resources
    /// (e.g. a Kafka producer, JDBC connection pool). Called once
    /// before the first `write`. Mirrors `SourceReader::open`.
    fn open(&mut self) -> WriterFuture<'_, ()> {
        Box::pin(async { Ok(()) })
    }

    fn write(&mut self, record: Self::Input) -> WriterFuture<'_, ()>;
    fn prepare_commit(&mut self) -> WriterFuture<'_, Vec<Self::CommitInfo>>;
    fn snapshot_state(&mut self) -> WriterFuture<'_, Vec<u8>>;
    fn close(&mut self) -> WriterFuture<'_, ()>;
}
