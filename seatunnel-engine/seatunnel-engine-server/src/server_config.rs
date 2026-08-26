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

//! Engine server startup configuration (`seatunnel.yaml`), mirroring the
//! Java Zeta engine's config file where the concepts map onto this
//! project:
//!
//! ```yaml
//! seatunnel:
//!   engine:
//!     history-job-expire-minutes: 1440        # Java key, same meaning
//!     checkpoint:
//!       interval: 30000                       # engine default; job env overrides
//!       keep-checkpoint-count: 3              # Java key
//!       storage:
//!         type: localfile                     # Java key (only localfile here)
//!         namespace: .seatunnel-state         # Java key → state dir
//!         auto-clean: true
//!         clean-grace-minutes: 10
//!         clean-interval-minutes: 60
//! ```
//!
//! Precedence: CLI flags and env vars override the file; the file
//! overrides built-in defaults. Loading is failure-tolerant: a missing
//! file keeps defaults; a malformed file fails startup loudly.

use serde::Deserialize;

/// Root of `seatunnel.yaml`.
#[derive(Debug, Clone, Default, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub struct ServerConfigFile {
    #[serde(default)]
    pub seatunnel: SeatunnelSection,
}

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub struct SeatunnelSection {
    #[serde(default)]
    pub engine: EngineSection,
}

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub struct EngineSection {
    /// Java `history-job-expire-minutes`: how long a finished job's
    /// artifacts are kept before expiry (default 1440 = 24h).
    #[serde(default)]
    pub history_job_expire_minutes: Option<u64>,
    #[serde(default)]
    pub checkpoint: CheckpointSection,
}

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub struct CheckpointSection {
    /// Engine-wide default checkpoint interval in ms; a job's
    /// `env.checkpoint.interval` overrides it.
    #[serde(default)]
    pub interval: Option<u64>,
    /// Java `keep-checkpoint-count`: snapshots retained per task.
    #[serde(default)]
    pub keep_checkpoint_count: Option<usize>,
    #[serde(default)]
    pub storage: StorageSection,
}

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub struct StorageSection {
    /// Java `checkpoint.storage.type`; only `localfile` exists here.
    #[serde(default)]
    pub r#type: Option<String>,
    /// Java `checkpoint.storage.plugin-config.namespace` analogue: the
    /// local directory for checkpoint files.
    #[serde(default)]
    pub namespace: Option<String>,
    /// Delete a terminal job's state automatically (cancel after grace,
    /// TTL sweep for everything else). Default true.
    #[serde(default)]
    pub auto_clean: Option<bool>,
    /// Grace period after a job is cancelled before its local state is
    /// deleted (restore window for operator intervention).
    #[serde(default)]
    pub clean_grace_minutes: Option<u64>,
    /// How often the TTL sweep runs.
    #[serde(default)]
    pub clean_interval_minutes: Option<u64>,
}

/// Resolved effective configuration (file + overrides merged).
#[derive(Debug, Clone)]
pub struct EngineServerConfig {
    /// Local checkpoint state directory.
    pub state_dir: String,
    /// Snapshots retained per task on disk.
    pub keep_checkpoint_count: usize,
    /// Engine default checkpoint interval (ms).
    pub checkpoint_interval: u64,
    /// Terminal-job state auto cleanup enabled.
    pub auto_clean: bool,
    /// Minutes after a cancelled job's state is deleted.
    pub clean_grace_minutes: u64,
    /// Minutes between TTL sweeps.
    pub clean_interval_minutes: u64,
    /// Minutes of inactivity before a job's state dir is swept
    /// (Java `history-job-expire-minutes`).
    pub history_job_expire_minutes: u64,
}

impl Default for EngineServerConfig {
    fn default() -> Self {
        EngineServerConfig {
            state_dir: ".seatunnel-state".to_string(),
            keep_checkpoint_count: 3,
            checkpoint_interval: 30_000,
            auto_clean: true,
            clean_grace_minutes: 10,
            clean_interval_minutes: 60,
            history_job_expire_minutes: 1440,
        }
    }
}

impl EngineServerConfig {
    /// Load `seatunnel.yaml` from `path` (if present) and apply the
    /// CLI/env overrides. `None` for an override leaves the file value
    /// (or built-in default) in place.
    pub fn load(
        path: Option<&str>,
        cli_state_dir: Option<&str>,
        env_state_dir: Option<&str>,
    ) -> anyhow::Result<Self> {
        let mut config = EngineServerConfig::default();
        if let Some(path) = path {
            let raw = std::fs::read_to_string(path)
                .map_err(|e| anyhow::anyhow!("cannot read engine config '{}': {}", path, e))?;
            let file: ServerConfigFile = serde_yaml::from_str(&raw)
                .map_err(|e| anyhow::anyhow!("cannot parse engine config '{}': {}", path, e))?;
            config.apply_file(&file);
        }
        if let Some(dir) = env_state_dir.filter(|d| !d.is_empty()) {
            config.state_dir = dir.to_string();
        }
        if let Some(dir) = cli_state_dir.filter(|d| !d.is_empty()) {
            config.state_dir = dir.to_string();
        }
        Ok(config)
    }

    fn apply_file(&mut self, file: &ServerConfigFile) {
        let engine = &file.seatunnel.engine;
        if let Some(minutes) = engine.history_job_expire_minutes {
            self.history_job_expire_minutes = minutes.max(1);
        }
        if let Some(interval) = engine.checkpoint.interval {
            self.checkpoint_interval = interval.max(1);
        }
        if let Some(count) = engine.checkpoint.keep_checkpoint_count {
            self.keep_checkpoint_count = count.max(1);
        }
        let storage = &engine.checkpoint.storage;
        if let Some(namespace) = storage.namespace.as_deref().filter(|n| !n.is_empty()) {
            self.state_dir = namespace.to_string();
        }
        if let Some(auto) = storage.auto_clean {
            self.auto_clean = auto;
        }
        if let Some(minutes) = storage.clean_grace_minutes {
            self.clean_grace_minutes = minutes;
        }
        if let Some(minutes) = storage.clean_interval_minutes {
            self.clean_interval_minutes = minutes.max(1);
        }
        if let Some(kind) = storage.r#type.as_deref() {
            if !kind.is_empty() && kind != "localfile" {
                tracing::warn!(
                    "checkpoint.storage.type '{}' ignored — this engine stores checkpoints \
                     on the local filesystem (localfile) only",
                    kind
                );
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn defaults_without_file() {
        let config = EngineServerConfig::load(None, None, None).unwrap();
        assert_eq!(config.state_dir, ".seatunnel-state");
        assert_eq!(config.keep_checkpoint_count, 3);
        assert!(config.auto_clean);
        assert_eq!(config.history_job_expire_minutes, 1440);
    }

    #[test]
    fn parses_java_style_keys() {
        let yaml = r#"
seatunnel:
  engine:
    history-job-expire-minutes: 720
    checkpoint:
      interval: 15000
      keep-checkpoint-count: 5
      storage:
        type: localfile
        namespace: /data/seatunnel/state
        auto-clean: true
        clean-grace-minutes: 2
        clean-interval-minutes: 15
"#;
        let file: ServerConfigFile = serde_yaml::from_str(yaml).unwrap();
        let mut config = EngineServerConfig::default();
        config.apply_file(&file);
        assert_eq!(config.history_job_expire_minutes, 720);
        assert_eq!(config.checkpoint_interval, 15000);
        assert_eq!(config.keep_checkpoint_count, 5);
        assert_eq!(config.state_dir, "/data/seatunnel/state");
        assert_eq!(config.clean_grace_minutes, 2);
        assert_eq!(config.clean_interval_minutes, 15);
    }

    #[test]
    fn auto_clean_can_be_disabled() {
        let yaml = "seatunnel:\n  engine:\n    checkpoint:\n      storage:\n        auto-clean: false\n";
        let file: ServerConfigFile = serde_yaml::from_str(yaml).unwrap();
        let mut config = EngineServerConfig::default();
        config.apply_file(&file);
        assert!(!config.auto_clean);
    }

    #[test]
    fn overrides_beat_file() {
        let yaml = "seatunnel:\n  engine:\n    checkpoint:\n      storage:\n        namespace: /from-file\n";
        let file: ServerConfigFile = serde_yaml::from_str(yaml).unwrap();
        let mut config = EngineServerConfig::default();
        config.apply_file(&file);
        assert_eq!(config.state_dir, "/from-file");
        // CLI overrides the file.
        let config = EngineServerConfig::load(None, Some("/from-cli"), None).unwrap();
        assert_eq!(config.state_dir, "/from-cli");
        // Env sits between the two.
        let config = EngineServerConfig::load(None, None, Some("/from-env")).unwrap();
        assert_eq!(config.state_dir, "/from-env");
    }
}
