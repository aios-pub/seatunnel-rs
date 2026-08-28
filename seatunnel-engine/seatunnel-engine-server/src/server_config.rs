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

/// Top-level `hazelcast:` section (Java cluster config adapted).
#[derive(Debug, Clone, Default, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub struct HazelcastSection {
    #[serde(default)]
    pub cluster_name: Option<String>,
    #[serde(default)]
    pub network: HazelcastNetwork,
}

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub struct HazelcastNetwork {
    #[serde(default)]
    pub join: HazelcastJoin,
    #[serde(default)]
    pub port: HazelcastPort,
}

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub struct HazelcastJoin {
    #[serde(default)]
    pub tcp_ip: HazelcastTcpIp,
}

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub struct HazelcastTcpIp {
    #[serde(default)]
    pub enabled: Option<bool>,
    #[serde(default)]
    pub member_list: Vec<String>,
}

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub struct HazelcastPort {
    #[serde(default)]
    pub auto_increment: Option<bool>,
    #[serde(default)]
    pub port: Option<u16>,
}

/// Root of `seatunnel.yaml`.
#[derive(Debug, Clone, Default, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub struct ServerConfigFile {
    #[serde(default)]
    pub seatunnel: SeatunnelSection,
    #[serde(default)]
    pub hazelcast: HazelcastSection,
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
    /// Soft heartbeat timeout (ms): a worker silent longer than this is
    /// suspected — no NEW tasks are assigned to it, but its running tasks
    /// and registry entry stay until the hard timeout.
    #[serde(default)]
    pub worker_soft_timeout_ms: Option<u64>,
    /// Hard heartbeat timeout before a worker is evicted and its tasks
    /// become claimable (failover). Defaults deliberately conservative:
    /// a long GC pause or network jitter must not trigger a false
    /// eviction (the Java engine learned this the hard way with 27s
    /// full-GC stalls crossing a 60s timeout).
    #[serde(default)]
    pub worker_timeout_ms: Option<u64>,
    /// Worker → master heartbeat period (ms). The master may override it
    /// per response (`next_interval_ms`).
    #[serde(default)]
    pub heartbeat_interval_ms: Option<u64>,
    /// Task slot budget this worker advertises (Java `slot-num` analogue;
    /// scheduling use lands with least-loaded placement).
    #[serde(default)]
    pub slot_num: Option<u32>,
    /// Master-to-master state replication period (HA standby sync).
    #[serde(default)]
    pub replication_interval_ms: Option<u64>,
    /// This worker's advertised address (host:port).
    #[serde(default)]
    pub worker_address: Option<String>,
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
    /// S3 backend keys (`checkpoint.storage.type: s3`).
    #[serde(default)]
    pub plugin_config: S3PluginConfig,
}

/// `checkpoint.storage.plugin-config` block (S3/MinIO).
#[derive(Debug, Clone, Default, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub struct S3PluginConfig {
    #[serde(default)]
    pub bucket: Option<String>,
    /// S3-compatible endpoint (MinIO: http://host:9000).
    #[serde(default)]
    pub endpoint: Option<String>,
    #[serde(default)]
    pub region: Option<String>,
    #[serde(default)]
    pub access_key: Option<String>,
    #[serde(default)]
    pub secret_key: Option<String>,
    /// Object key prefix inside the bucket.
    #[serde(default)]
    pub prefix: Option<String>,
    /// MinIO needs path-style addressing.
    #[serde(default)]
    pub path_style: Option<bool>,
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
    /// Heartbeat silence after which a worker is suspected (ms). Suspect
    /// workers receive no new task assignments.
    pub worker_soft_timeout_ms: u64,
    /// Heartbeat timeout before worker eviction (ms).
    pub worker_timeout_ms: u64,
    /// Worker → master heartbeat period (ms).
    pub heartbeat_interval_ms: u64,
    /// Task slot budget advertised by this worker.
    pub slot_num: u32,
    /// Master state replication period (ms).
    pub replication_interval_ms: u64,
    /// This worker's advertised address.
    pub worker_address: String,
    /// Checkpoint storage backend: localfile | master | s3.
    pub storage_type: String,
    /// S3 backend settings (when storage_type = s3).
    pub s3: ResolvedS3Config,
    /// Ordered master seed addresses (cluster member list).
    pub member_list: Vec<String>,
    /// Cluster name (informational fencing, Java key).
    pub cluster_name: String,
    /// Master bind port from the hazelcast section (None = keep default).
    pub hazelcast_port: Option<u16>,
}

/// Resolved S3/MinIO checkpoint storage configuration.
#[derive(Debug, Clone, Default)]
pub struct ResolvedS3Config {
    pub bucket: String,
    pub endpoint: String,
    pub region: String,
    pub access_key: Option<String>,
    pub secret_key: Option<String>,
    pub prefix: String,
    pub path_style: bool,
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
            // Conservative failure detection: suspect at 30s, evict at 60s.
            // (The old 6s default evicted workers on a single long GC
            // pause — the Java engine ships 180s for exactly this reason.)
            worker_soft_timeout_ms: 30_000,
            worker_timeout_ms: 60_000,
            heartbeat_interval_ms: 2_000,
            slot_num: 8,
            replication_interval_ms: 5_000,
            worker_address: "127.0.0.1:5001".to_string(),
            storage_type: "localfile".to_string(),
            s3: ResolvedS3Config::default(),
            member_list: vec!["127.0.0.1:5800".to_string()],
            cluster_name: "seatunnel".to_string(),
            hazelcast_port: None,
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
        if let Some(ms) = engine.worker_soft_timeout_ms {
            self.worker_soft_timeout_ms = ms.max(1_000);
        }
        if let Some(ms) = engine.worker_timeout_ms {
            // The hard timeout must never sit below the soft threshold.
            self.worker_timeout_ms = ms.max(self.worker_soft_timeout_ms).max(1_000);
        }
        if let Some(ms) = engine.heartbeat_interval_ms {
            self.heartbeat_interval_ms = ms.clamp(250, 60_000);
        }
        if let Some(slots) = engine.slot_num {
            self.slot_num = slots.max(1);
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
            if !kind.is_empty() {
                match kind {
                    "localfile" | "master" | "s3" => self.storage_type = kind.to_string(),
                    other => tracing::warn!(
                        "checkpoint.storage.type '{}' unknown, using localfile",
                        other
                    ),
                }
            }
        }
        let plugin = &storage.plugin_config;
        if let Some(bucket) = plugin.bucket.as_deref().filter(|b| !b.is_empty()) {
            self.s3.bucket = bucket.to_string();
        }
        if let Some(endpoint) = plugin.endpoint.as_deref().filter(|e| !e.is_empty()) {
            self.s3.endpoint = endpoint.to_string();
        }
        if let Some(region) = plugin.region.as_deref().filter(|r| !r.is_empty()) {
            self.s3.region = region.to_string();
        }
        if let Some(key) = plugin.access_key.as_deref().filter(|k| !k.is_empty()) {
            self.s3.access_key = Some(key.to_string());
        }
        if let Some(key) = plugin.secret_key.as_deref().filter(|k| !k.is_empty()) {
            self.s3.secret_key = Some(key.to_string());
        }
        if let Some(prefix) = plugin.prefix.as_deref().filter(|p| !p.is_empty()) {
            self.s3.prefix = prefix.trim_matches('/').to_string();
        }
        if let Some(path_style) = plugin.path_style {
            self.s3.path_style = path_style;
        }
        // hazelcast section
        let hz = &file.hazelcast;
        if let Some(name) = hz.cluster_name.as_deref().filter(|n| !n.is_empty()) {
            self.cluster_name = name.to_string();
        }
        if let Some(port) = hz.network.port.port {
            self.hazelcast_port = Some(port);
        }
        if hz.network.port.auto_increment == Some(true) {
            tracing::warn!(
                "hazelcast.network.port.auto-increment is not supported — \
                 member-list requires deterministic master ports"
            );
        }
        let members = hz
            .network
            .join
            .tcp_ip
            .member_list
            .iter()
            .map(|m| m.trim().to_string())
            .filter(|m| !m.is_empty())
            .collect::<Vec<_>>();
        if !members.is_empty() {
            self.member_list = members;
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
        assert_eq!(config.worker_soft_timeout_ms, 30_000);
        assert_eq!(config.worker_timeout_ms, 60_000);
        assert_eq!(config.heartbeat_interval_ms, 2_000);
        assert_eq!(config.slot_num, 8);
    }

    #[test]
    fn timeout_overrides_are_clamped() {
        let yaml = "
seatunnel:
  engine:
    worker-soft-timeout-ms: 5000
    worker-timeout-ms: 2000
    heartbeat-interval-ms: 500
    slot-num: 16
";
        let file: ServerConfigFile = serde_yaml::from_str(yaml).unwrap();
        let mut config = EngineServerConfig::default();
        config.apply_file(&file);
        assert_eq!(config.worker_soft_timeout_ms, 5_000);
        // Hard timeout lifted to the soft threshold (2000 < 5000).
        assert_eq!(config.worker_timeout_ms, 5_000);
        assert_eq!(config.heartbeat_interval_ms, 500);
        assert_eq!(config.slot_num, 16);
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
        let yaml =
            "seatunnel:\n  engine:\n    checkpoint:\n      storage:\n        auto-clean: false\n";
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
