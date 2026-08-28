/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.
 */

//! Per-task in-memory log ring buffer.
//!
//! Task execution records lifecycle events (start, checkpoints, errors,
//! sampled data rows) here; the worker ships increments to the master in
//! heartbeats so the web console can show live per-task logs without
//! touching worker stdout.

use std::collections::VecDeque;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use parking_lot::Mutex;

/// Default number of retained log lines per task.
pub const TASK_LOG_CAPACITY: usize = 500;

/// One captured log line.
#[derive(Debug, Clone, PartialEq)]
pub struct TaskLogEntry {
    /// Monotonic sequence number, used by the worker to ship only the
    /// increment since the previous heartbeat.
    pub seq: u64,
    pub timestamp_ms: i64,
    pub level: String,
    pub message: String,
}

impl TaskLogEntry {
    /// Render as `[YYYY-MM-DD HH:MM:SS][LEVEL] message` (no task prefix;
    /// callers add it when merging tasks).
    pub fn render(&self) -> String {
        format!(
            "[{}][{}] {}",
            format_time(self.timestamp_ms),
            self.level,
            self.message
        )
    }
}

struct Inner {
    entries: VecDeque<TaskLogEntry>,
    capacity: usize,
}

/// Shared bounded ring of task log lines.
#[derive(Clone)]
pub struct TaskLogRing {
    inner: Arc<Mutex<Inner>>,
    seq: Arc<AtomicU64>,
}

impl TaskLogRing {
    pub fn new(capacity: usize) -> Self {
        TaskLogRing {
            inner: Arc::new(Mutex::new(Inner {
                entries: VecDeque::with_capacity(capacity.clamp(16, 10_000)),
                capacity: capacity.clamp(16, 10_000),
            })),
            seq: Arc::new(AtomicU64::new(0)),
        }
    }

    /// Append one line. Also mirrors to tracing at info level so the
    /// worker log keeps a copy.
    pub fn push(&self, level: &str, message: impl Into<String>) {
        let entry = TaskLogEntry {
            seq: self.seq.fetch_add(1, Ordering::Relaxed),
            timestamp_ms: crate::now_millis(),
            level: level.to_string(),
            message: message.into(),
        };
        tracing::debug!(target: "task_log", "[{}] {}", entry.level, entry.message);
        let mut inner = self.inner.lock();
        if inner.entries.len() == inner.capacity {
            inner.entries.pop_front();
        }
        inner.entries.push_back(entry);
    }

    /// Shorthand for `push("INFO", ..)`.
    pub fn info(&self, message: impl Into<String>) {
        self.push("INFO", message);
    }

    /// Shorthand for `push("WARN", ..)`.
    pub fn warn(&self, message: impl Into<String>) {
        self.push("WARN", message);
    }

    /// Shorthand for `push("ERROR", ..)`.
    pub fn error(&self, message: impl Into<String>) {
        self.push("ERROR", message);
    }

    /// Entries with `seq >= cursor` — the increment accumulated since the
    /// caller last shipped. Use [`TaskLogRing::cursor`] as the bookmark:
    /// it equals the number of entries ever assigned, so an empty ring
    /// starts at 0 and nothing is ever skipped or re-sent.
    pub fn entries_after(&self, cursor: u64) -> Vec<TaskLogEntry> {
        self.inner
            .lock()
            .entries
            .iter()
            .filter(|e| e.seq >= cursor)
            .cloned()
            .collect()
    }

    /// Full snapshot, oldest first.
    pub fn snapshot(&self) -> Vec<TaskLogEntry> {
        self.inner.lock().entries.iter().cloned().collect()
    }

    /// Shipping bookmark: pass it to [`TaskLogRing::entries_after`] on the
    /// next heartbeat to fetch exactly the new lines.
    pub fn cursor(&self) -> u64 {
        self.seq.load(Ordering::Relaxed)
    }
}

/// `YYYY-MM-DD HH:MM:SS` from epoch-ms, in the local timezone.
fn format_time(ms: i64) -> String {
    use chrono::TimeZone;
    chrono::Local
        .timestamp_millis_opt(ms)
        .single()
        .map(|t| t.format("%Y-%m-%d %H:%M:%S").to_string())
        .unwrap_or_else(|| ms.to_string())
}

impl Default for TaskLogRing {
    fn default() -> Self {
        Self::new(TASK_LOG_CAPACITY)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ring_keeps_capacity_and_orders_entries() {
        let ring = TaskLogRing::new(20);
        for i in 0..25 {
            ring.info(format!("line-{}", i));
        }
        let snapshot = ring.snapshot();
        assert_eq!(snapshot.len(), 20);
        assert_eq!(snapshot[0].message, "line-5");
        assert_eq!(snapshot[19].message, "line-24");
    }

    #[test]
    fn incremental_fetch_after_cursor() {
        let ring = TaskLogRing::new(100);
        ring.info("a");
        let cursor = ring.cursor();
        ring.info("b");
        ring.info("c");
        let delta = ring.entries_after(cursor);
        assert_eq!(delta.len(), 2);
        assert_eq!(delta[0].message, "b");
        assert_eq!(delta[1].message, "c");
        // Shipping the updated cursor again yields nothing new.
        let cursor = ring.cursor();
        assert!(ring.entries_after(cursor).is_empty());
    }

    #[test]
    fn render_contains_level() {
        let ring = TaskLogRing::new(10);
        ring.error("boom");
        let line = ring.snapshot()[0].render();
        assert!(line.contains("[ERROR] boom"));
    }
}
