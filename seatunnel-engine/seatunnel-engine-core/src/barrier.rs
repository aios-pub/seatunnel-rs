/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.
 */

use std::collections::{HashMap, HashSet, VecDeque};
use std::fmt;

/// A checkpoint barrier carries the checkpoint id and timestamp.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CheckpointBarrier {
    pub checkpoint_id: u64,
    pub timestamp: i64,
}

impl CheckpointBarrier {
    pub fn new(checkpoint_id: u64, timestamp: i64) -> Self {
        CheckpointBarrier {
            checkpoint_id,
            timestamp,
        }
    }
}

impl fmt::Display for CheckpointBarrier {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "CP#{}@{}", self.checkpoint_id, self.timestamp)
    }
}

/// Types of control messages in the stream.
#[derive(Debug, Clone)]
pub enum StreamElement {
    Data(Vec<u8>),
    CheckpointBarrier(CheckpointBarrier),
    SchemaChange(String),
    EndOfStream,
}

impl fmt::Display for StreamElement {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            StreamElement::Data(_) => write!(f, "Data"),
            StreamElement::CheckpointBarrier(b) => write!(f, "{}", b),
            StreamElement::SchemaChange(s) => write!(f, "SchemaChange({})", s),
            StreamElement::EndOfStream => write!(f, "EOS"),
        }
    }
}

/// Barrier alignment tracking for a single task.
#[derive(Debug, Clone)]
pub struct BarrierTracker {
    pub task_id: String,
    pub parallelism: usize,
    pub received_counts: HashMap<u64, usize>,
    pub aligned: HashSet<u64>,
    pub pending_data: VecDeque<StreamElement>,
}

impl BarrierTracker {
    pub fn new(task_id: String, parallelism: usize) -> Self {
        BarrierTracker {
            task_id,
            parallelism,
            received_counts: HashMap::new(),
            aligned: HashSet::new(),
            pending_data: VecDeque::new(),
        }
    }

    /// Receive a stream element. Returns Some(barrier_id) if a barrier is aligned.
    pub fn receive(&mut self, element: StreamElement) -> Option<u64> {
        match &element {
            StreamElement::CheckpointBarrier(barrier) => {
                let cp_id = barrier.checkpoint_id;
                let count = self.received_counts.entry(cp_id).or_insert(0);
                *count += 1;
                if *count >= self.parallelism {
                    self.aligned.insert(cp_id);
                    return Some(cp_id);
                }
                None
            }
            _ => {
                self.pending_data.push_back(element);
                None
            }
        }
    }

    pub fn is_aligned(&self, checkpoint_id: u64) -> bool {
        self.aligned.contains(&checkpoint_id)
    }

    pub fn pending_count(&self) -> usize {
        self.pending_data.len()
    }

    pub fn drain_pending(&mut self) -> Vec<StreamElement> {
        std::mem::take(&mut self.pending_data).into_iter().collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_barrier_alignment() {
        let mut tracker = BarrierTracker::new("task-0".to_string(), 3);
        tracker.receive(StreamElement::CheckpointBarrier(CheckpointBarrier::new(
            1, 1000,
        )));
        tracker.receive(StreamElement::CheckpointBarrier(CheckpointBarrier::new(
            1, 1000,
        )));
        assert_eq!(
            tracker.receive(StreamElement::CheckpointBarrier(CheckpointBarrier::new(
                1, 1000
            ))),
            Some(1)
        );
        assert!(tracker.is_aligned(1));
    }

    #[test]
    fn test_pending_data() {
        let mut tracker = BarrierTracker::new("task-0".to_string(), 2);
        tracker.receive(StreamElement::Data(b"hello".to_vec()));
        tracker.receive(StreamElement::Data(b"world".to_vec()));
        assert_eq!(tracker.pending_count(), 2);
        tracker.receive(StreamElement::CheckpointBarrier(CheckpointBarrier::new(
            1, 1000,
        )));
        tracker.receive(StreamElement::CheckpointBarrier(CheckpointBarrier::new(
            1, 1000,
        )));
    }

    #[test]
    fn test_multiple_barriers() {
        let mut tracker = BarrierTracker::new("task-0".to_string(), 2);
        tracker.receive(StreamElement::CheckpointBarrier(CheckpointBarrier::new(
            1, 1000,
        )));
        tracker.receive(StreamElement::CheckpointBarrier(CheckpointBarrier::new(
            1, 1000,
        )));
        assert!(tracker.is_aligned(1));
        tracker.receive(StreamElement::CheckpointBarrier(CheckpointBarrier::new(
            2, 2000,
        )));
        tracker.receive(StreamElement::CheckpointBarrier(CheckpointBarrier::new(
            2, 2000,
        )));
        assert!(tracker.is_aligned(2));
    }

    #[test]
    fn test_display() {
        let barrier = CheckpointBarrier::new(5, 5000);
        assert_eq!(barrier.to_string(), "CP#5@5000");
        assert_eq!(StreamElement::EndOfStream.to_string(), "EOS");
    }
}
