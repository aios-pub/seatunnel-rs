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

use std::future::Future;
use std::pin::Pin;

/// Boxed future returned by async [`SinkCommitter`] operations.
pub type CommitterFuture<'a, T> = Pin<Box<dyn Future<Output = anyhow::Result<T>> + Send + 'a>>;

pub trait SinkCommitter: Send {
    type CommitInfo: serde::Serialize + Send + Sync;
    type AggregatedCommitInfo: serde::Serialize + Send + Sync;

    fn commit(
        &mut self,
        commit_infos: Vec<Self::CommitInfo>,
    ) -> CommitterFuture<'_, Self::AggregatedCommitInfo>;
    fn abort(&mut self, commit_infos: Vec<Self::CommitInfo>) -> CommitterFuture<'_, ()>;
}
