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
    /// DEPRECATED (ignored): static slot budgets are gone. Admission is
    /// dynamic — measured event-loop lag and memory watermark.
    #[serde(default)]
    pub slot_num: Option<u32>,
    /// Overload threshold for the event-loop-lag signal (ms). A worker
    /// whose lag EMA reaches this stops accepting new tasks (with
    /// hysteresis). 0 disables the signal.
    #[serde(default)]
    pub overload_lag_ms: Option<u64>,
    /// Memory watermark (percent 0-100): process RSS over usable memory
    /// (cgroup v2 limit when present, else physical RAM). 0 disables.
    #[serde(default)]
    pub memory_watermark_percent: Option<u64>,
    /// Recovery hysteresis: an overloaded worker accepts again only after
    /// every signal stayed healthy for this many seconds.
    #[serde(default)]
    pub overload_cooldown_secs: Option<u64>,
    /// Master-side rate fuse for the 1-3s measurement blind window: max
    /// tasks handed to one worker per heartbeat. 0 = unlimited. This is a
    /// RATE, not a slot count.
    #[serde(default)]
    pub dispatch_batch_limit: Option<u32>,
    /// Coordinated-checkpoint timeout (ms): a triggered checkpoint that
    /// has not collected every participating task's prepare by then is
    /// aborted (Java `checkpoint.timeout` analogue).
    #[serde(default)]
    pub checkpoint_timeout_ms: Option<u64>,
    /// Cancel deadline (ms): a cancelled job's tasks still non-terminal
    /// on the master after this long are forced CANCELLED, so the cancel
    /// broadcast cannot ride every heartbeat forever (hung task, lost
    /// terminal report).
    #[serde(default)]
    pub cancel_force_timeout_ms: Option<u64>,
    /// Master-to-master state replication period (HA standby sync).
    #[serde(default)]
    pub replication_interval_ms: Option<u64>,
    /// This worker's advertised address (host:port).
    #[serde(default)]
    pub worker_address: Option<String>,
    #[serde(default)]
    pub checkpoint: CheckpointSection,
    #[serde(default)]
    pub raft: RaftSection,
}

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub struct RaftSection {
    /// Election timeout window lower bound (ms). The effective window is
    /// shifted per node by `election-skew-ms` (see EngineServerConfig).
    #[serde(default)]
    pub election_timeout_min_ms: Option<u64>,
    /// Election timeout window upper bound (ms).
    #[serde(default)]
    pub election_timeout_max_ms: Option<u64>,
    /// Per-node election window shift (ms): node i (member-list order,
    /// 1-based) draws from [min+(i-1)*skew, max+(i-1)*skew). Windows are
    /// disjoint when skew >= window width + one tick, making the lowest
    /// live member win every election — no split votes, no term storms.
    /// 0 disables the skew.
    #[serde(default)]
    pub election_skew_ms: Option<u64>,
    /// Raft heartbeat period (ms). Also the hard RPC timeout openraft
    /// applies to append-entries, so it must exceed one RPC round trip.
    #[serde(default)]
    pub heartbeat_interval_ms: Option<u64>,
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
    /// Lag-signal overload threshold (ms; 0 disables).
    pub overload_lag_ms: u64,
    /// Memory watermark (percent; 0 disables).
    pub memory_watermark_percent: u64,
    /// Recovery hysteresis seconds.
    pub overload_cooldown_secs: u64,
    /// Max tasks handed to one worker per heartbeat (0 = unlimited).
    pub dispatch_batch_limit: u32,
    /// Coordinated-checkpoint abort timeout (ms).
    pub checkpoint_timeout_ms: u64,
    /// Cancel deadline (ms): a cancelled job's tasks still non-terminal
    /// on the master after this long are forced CANCELLED (stops the
    /// endless cancel re-broadcast).
    pub cancel_force_timeout_ms: u64,
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
    /// Raft election timing (see RaftSection for semantics).
    pub raft: RaftTiming,
}

/// Resolved raft election timing.
///
/// openraft draws the election timeout ONCE per process
/// (`gen_range(min..max)`, fixed forever), and all followers start
/// counting from the leader's last heartbeat — so two nodes that draw
/// close values campaign in lockstep forever: each bumps the term,
/// neither wins. Giving every node its own window, shifted by member
/// order, makes collisions structurally impossible: the lowest live
/// member always fires first and the others grant its vote.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RaftTiming {
    /// Window lower bound for node 1 (ms).
    pub election_timeout_min_ms: u64,
    /// Window upper bound for node 1 (ms).
    pub election_timeout_max_ms: u64,
    /// Per-node window shift (ms); 0 disables the skew.
    pub election_skew_ms: u64,
    /// Heartbeat period (ms).
    pub heartbeat_interval_ms: u64,
}

impl RaftTiming {
    /// The election timeout window of voter `node_id` (1-based position
    /// in the member list). Higher ids wait longer — member-list order
    /// is the leader priority order.
    pub fn election_window(&self, node_id: u64) -> (u64, u64) {
        let shift = self
            .election_skew_ms
            .saturating_mul(node_id.saturating_sub(1));
        (
            self.election_timeout_min_ms.saturating_add(shift),
            self.election_timeout_max_ms.saturating_add(shift),
        )
    }
}

impl Default for RaftTiming {
    fn default() -> Self {
        RaftTiming {
            election_timeout_min_ms: 900,
            election_timeout_max_ms: 1_300,
            // Windows must clear each other by at least one openraft
            // tick (heartbeat*3/2 = 225ms): skew 700 with a 400ms window
            // leaves a 300ms gap, so the lower node's vote request always
            // arrives before the next node's timer fires. Voter windows:
            // [900,1300), [1600,2000), [2300,2700), ...
            election_skew_ms: 700,
            heartbeat_interval_ms: 150,
        }
    }
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
            overload_lag_ms: 500,
            memory_watermark_percent: 75,
            overload_cooldown_secs: 10,
            dispatch_batch_limit: 16,
            checkpoint_timeout_ms: 30_000,
            cancel_force_timeout_ms: 300_000,
            replication_interval_ms: 5_000,
            worker_address: "127.0.0.1:5001".to_string(),
            storage_type: "localfile".to_string(),
            s3: ResolvedS3Config::default(),
            member_list: vec!["127.0.0.1:5800".to_string()],
            cluster_name: "seatunnel".to_string(),
            hazelcast_port: None,
            raft: RaftTiming::default(),
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

    /// The worker's admission thresholds.
    pub fn admission_config(&self) -> crate::admission::AdmissionConfig {
        crate::admission::AdmissionConfig {
            lag_threshold_ms: self.overload_lag_ms,
            memory_watermark_percent: self.memory_watermark_percent,
            cooldown_secs: self.overload_cooldown_secs,
        }
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
        if engine.slot_num.is_some() {
            tracing::warn!(
                "slot-num is deprecated and ignored: task admission is \
                 dynamic (event-loop lag + memory watermark)"
            );
        }
        if let Some(ms) = engine.overload_lag_ms {
            self.overload_lag_ms = ms;
        }
        if let Some(percent) = engine.memory_watermark_percent {
            self.memory_watermark_percent = percent.min(100);
        }
        if let Some(secs) = engine.overload_cooldown_secs {
            self.overload_cooldown_secs = secs;
        }
        if let Some(limit) = engine.dispatch_batch_limit {
            self.dispatch_batch_limit = limit;
        }
        if let Some(ms) = engine.checkpoint_timeout_ms {
            self.checkpoint_timeout_ms = ms.max(1_000);
        }
        if let Some(ms) = engine.cancel_force_timeout_ms {
            // Kept well above the heartbeat cadence so a healthy cancel
            // (which reports terminal within seconds) is never forced.
            self.cancel_force_timeout_ms = ms.max(30_000);
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
        let raft = &engine.raft;
        if let Some(ms) = raft.heartbeat_interval_ms {
            self.raft.heartbeat_interval_ms = ms.max(50);
        }
        if let Some(ms) = raft.election_skew_ms {
            self.raft.election_skew_ms = ms;
        }
        if let (Some(min), Some(max)) = (raft.election_timeout_min_ms, raft.election_timeout_max_ms)
        {
            // Require a strictly increasing window; lift max to min+1
            // rather than rejecting the whole config file.
            self.raft.election_timeout_min_ms = min.max(1);
            self.raft.election_timeout_max_ms = max.max(min + 1);
        } else {
            if let Some(min) = raft.election_timeout_min_ms {
                self.raft.election_timeout_min_ms = min.max(1);
            }
            if let Some(max) = raft.election_timeout_max_ms {
                self.raft.election_timeout_max_ms = max.max(self.raft.election_timeout_min_ms + 1);
            }
        }
        // A positive skew must actually separate the windows: lift it to
        // window width + one openraft tick, otherwise adjacent nodes can
        // still campaign in the same window (split votes return).
        let width = self.raft.election_timeout_max_ms - self.raft.election_timeout_min_ms;
        let tick = self.raft.heartbeat_interval_ms * 3 / 2;
        let min_skew = width + tick;
        if self.raft.election_skew_ms > 0 && self.raft.election_skew_ms < min_skew {
            tracing::warn!(
                "raft.election-skew-ms {} too small for window {}ms + tick {}ms; \
                 lifting to {} so per-node election windows stay disjoint",
                self.raft.election_skew_ms,
                width,
                tick,
                min_skew
            );
            self.raft.election_skew_ms = min_skew;
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
        assert_eq!(config.overload_lag_ms, 500);
        assert_eq!(config.memory_watermark_percent, 75);
        assert_eq!(config.overload_cooldown_secs, 10);
        assert_eq!(config.dispatch_batch_limit, 16);
        assert_eq!(config.cancel_force_timeout_ms, 300_000);
    }

    #[test]
    fn cancel_force_timeout_is_clamped() {
        let yaml = "
seatunnel:
  engine:
    cancel-force-timeout-ms: 5000
";
        let file: ServerConfigFile = serde_yaml::from_str(yaml).unwrap();
        let mut config = EngineServerConfig::default();
        config.apply_file(&file);
        // Lifted to the floor: a healthy cancel reports terminal within
        // seconds and must never be forced.
        assert_eq!(config.cancel_force_timeout_ms, 30_000);

        let yaml = "
seatunnel:
  engine:
    cancel-force-timeout-ms: 120000
";
        let file: ServerConfigFile = serde_yaml::from_str(yaml).unwrap();
        let mut config = EngineServerConfig::default();
        config.apply_file(&file);
        assert_eq!(config.cancel_force_timeout_ms, 120_000);
    }

    #[test]
    fn timeout_overrides_are_clamped() {
        let yaml = "
seatunnel:
  engine:
    worker-soft-timeout-ms: 5000
    worker-timeout-ms: 2000
    heartbeat-interval-ms: 500
    overload-lag-ms: 900
    memory-watermark-percent: 60
    overload-cooldown-secs: 30
    dispatch-batch-limit: 4
";
        let file: ServerConfigFile = serde_yaml::from_str(yaml).unwrap();
        let mut config = EngineServerConfig::default();
        config.apply_file(&file);
        assert_eq!(config.worker_soft_timeout_ms, 5_000);
        // Hard timeout lifted to the soft threshold (2000 < 5000).
        assert_eq!(config.worker_timeout_ms, 5_000);
        assert_eq!(config.heartbeat_interval_ms, 500);
        assert_eq!(config.overload_lag_ms, 900);
        assert_eq!(config.memory_watermark_percent, 60);
        assert_eq!(config.overload_cooldown_secs, 30);
        assert_eq!(config.dispatch_batch_limit, 4);
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

    #[test]
    fn raft_defaults_without_file() {
        let config = EngineServerConfig::load(None, None, None).unwrap();
        assert_eq!(config.raft.election_timeout_min_ms, 900);
        assert_eq!(config.raft.election_timeout_max_ms, 1_300);
        assert_eq!(config.raft.election_skew_ms, 700);
        assert_eq!(config.raft.heartbeat_interval_ms, 150);
    }

    #[test]
    fn raft_section_overrides_and_clamps() {
        let yaml = "
seatunnel:
  engine:
    raft:
      election-timeout-min-ms: 1000
      election-timeout-max-ms: 1500
      election-skew-ms: 0
      heartbeat-interval-ms: 30
";
        let file: ServerConfigFile = serde_yaml::from_str(yaml).unwrap();
        let mut config = EngineServerConfig::default();
        config.apply_file(&file);
        assert_eq!(config.raft.election_timeout_min_ms, 1_000);
        assert_eq!(config.raft.election_timeout_max_ms, 1_500);
        // Skew 0 disables the per-node priority shift.
        assert_eq!(config.raft.election_skew_ms, 0);
        // Heartbeat floored at 50ms (openraft append hard timeout).
        assert_eq!(config.raft.heartbeat_interval_ms, 50);
    }

    #[test]
    fn raft_window_ordering_is_repaired() {
        let yaml = "
seatunnel:
  engine:
    raft:
      election-timeout-min-ms: 1200
      election-timeout-max-ms: 1000
";
        let file: ServerConfigFile = serde_yaml::from_str(yaml).unwrap();
        let mut config = EngineServerConfig::default();
        config.apply_file(&file);
        assert!(config.raft.election_timeout_max_ms > config.raft.election_timeout_min_ms);
    }

    #[test]
    fn raft_small_positive_skew_is_lifted() {
        let yaml = "
seatunnel:
  engine:
    raft:
      election-skew-ms: 100
";
        let file: ServerConfigFile = serde_yaml::from_str(yaml).unwrap();
        let mut config = EngineServerConfig::default();
        config.apply_file(&file);
        let width = config.raft.election_timeout_max_ms - config.raft.election_timeout_min_ms;
        let tick = config.raft.heartbeat_interval_ms * 3 / 2;
        // 100ms skew cannot separate the default window — lifted to
        // width + tick so windows stay disjoint.
        assert_eq!(config.raft.election_skew_ms, width + tick);
    }

    #[test]
    fn raft_election_windows_are_disjoint() {
        let timing = RaftTiming::default();
        let tick = timing.heartbeat_interval_ms * 3 / 2; // openraft tick
        let windows: Vec<(u64, u64)> = (1..=5).map(|i| timing.election_window(i)).collect();
        for w in &windows {
            assert!(w.0 < w.1, "window must be non-empty: {:?}", w);
        }
        for pair in windows.windows(2) {
            // Gap between adjacent windows must clear one tick so the
            // lower node's vote request always arrives first.
            assert!(
                pair[1].0 >= pair[0].1 + tick,
                "adjacent windows overlap or gap < tick: {:?}",
                pair
            );
        }
        // Node 1 window is the unshifted default.
        assert_eq!(windows[0], (900, 1_300));
    }

    #[test]
    fn raft_zero_skew_disables_windows() {
        let timing = RaftTiming {
            election_skew_ms: 0,
            ..RaftTiming::default()
        };
        assert_eq!(timing.election_window(1), timing.election_window(3));
    }
}
