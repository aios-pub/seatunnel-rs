/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.
 */

use std::collections::HashMap;

use std::fmt;

/// DAG data model: Pipeline → Stage → Task
///
/// A Pipeline is a linear sequence of stages connected by data channels.
/// Each Stage contains one or more parallel Tasks (source, transform, or sink).
///
/// Top-level pipeline for a job.
#[derive(Debug, Clone)]
pub struct Pipeline {
    /// Unique pipeline identifier.
    pub id: String,
    /// Pipeline name.
    pub name: String,
    /// Ordered list of stages.
    pub stages: Vec<Stage>,
}

impl Pipeline {
    pub fn new(id: String, name: String) -> Self {
        Pipeline {
            id,
            name,
            stages: Vec::new(),
        }
    }

    pub fn add_stage(&mut self, stage: Stage) {
        self.stages.push(stage);
    }

    /// Returns the number of stages in this pipeline.
    pub fn stage_count(&self) -> usize {
        self.stages.len()
    }

    /// Returns the total parallelism across all stages.
    pub fn total_parallelism(&self) -> usize {
        self.stages.iter().map(|s| s.parallelism).sum()
    }

    /// Returns the source stage (first stage).
    pub fn source_stage(&self) -> Option<&Stage> {
        self.stages.first()
    }

    /// Returns the sink stage (last stage).
    pub fn sink_stage(&self) -> Option<&Stage> {
        self.stages.last()
    }
}

/// A stage in the pipeline (Source, Transform, or Sink).
#[derive(Debug, Clone)]
pub struct Stage {
    /// Unique stage identifier.
    pub id: String,
    /// Stage name.
    pub name: String,
    /// Stage type.
    pub stage_type: StageType,
    /// Number of parallel task instances.
    pub parallelism: usize,
    /// Configuration for this stage.
    pub config: HashMap<String, String>,
}

impl Stage {
    pub fn new(id: String, name: String, stage_type: StageType, parallelism: usize) -> Self {
        Stage {
            id,
            name,
            stage_type,
            parallelism,
            config: HashMap::new(),
        }
    }

    pub fn with_config(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.config.insert(key.into(), value.into());
        self
    }
}

/// The type of a stage in the pipeline.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StageType {
    Source,
    Transform,
    Sink,
}

impl fmt::Display for StageType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            StageType::Source => write!(f, "SOURCE"),
            StageType::Transform => write!(f, "TRANSFORM"),
            StageType::Sink => write!(f, "SINK"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_pipeline() {
        let mut pipeline = Pipeline::new("p1".to_string(), "test-pipeline".to_string());
        pipeline.add_stage(Stage::new(
            "s1".to_string(),
            "source".to_string(),
            StageType::Source,
            4,
        ));
        pipeline.add_stage(Stage::new(
            "s2".to_string(),
            "transform".to_string(),
            StageType::Transform,
            2,
        ));
        pipeline.add_stage(Stage::new(
            "s3".to_string(),
            "sink".to_string(),
            StageType::Sink,
            4,
        ));
        assert_eq!(pipeline.stage_count(), 3);
        assert_eq!(pipeline.total_parallelism(), 10);
        assert_eq!(pipeline.source_stage().unwrap().name, "source");
        assert_eq!(pipeline.sink_stage().unwrap().name, "sink");
    }
}
