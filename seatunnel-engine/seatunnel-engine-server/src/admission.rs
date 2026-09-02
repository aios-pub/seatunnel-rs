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

//! Dynamic task admission: "can this worker take another task?" is
//! answered by MEASURED pressure, never by a slot count.
//!
//! Two signals (each independently configurable and disable-able):
//! - **Event-loop lag**: a watchdog sleeps 1s and measures the overshoot.
//!   A saturated tokio runtime fires timers late — the honest "how busy
//!   am I" number (Node's event-loop lag / Kafka's handler-idle analogue).
//! - **Memory watermark**: process RSS over usable memory (cgroup v2
//!   limit when present, else physical RAM). Rust tasks are tiny but
//!   connector buffers are not (Kafka fetch buffers, CDC queues) — this
//!   measures the consequence, not a guess.
//!
//! Hysteresis: crossing a watermark takes effect immediately; recovery
//! requires every signal healthy for `overload_cooldown_secs` so a worker
//! at the boundary does not flap.
//!
//! There is deliberately NO capacity number. The only numeric guard is
//! the master-side dispatch batch limit per heartbeat — a rate fuse for
//! the 1-3s measurement blind window, not a slot budget.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

/// Admission thresholds (all fields 0 = signal disabled).
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct AdmissionConfig {
    /// Healthy while the lag EMA stays below this (ms). 0 disables.
    pub lag_threshold_ms: u64,
    /// Healthy while RSS/usable stays below this (percent 0-100). 0 disables.
    pub memory_watermark_percent: u64,
    /// Recovery requires this many consecutive healthy seconds.
    pub cooldown_secs: u64,
}

impl Default for AdmissionConfig {
    fn default() -> Self {
        AdmissionConfig {
            lag_threshold_ms: 500,
            memory_watermark_percent: 75,
            cooldown_secs: 10,
        }
    }
}

/// The measured signals at one point in time.
#[derive(Debug, Clone, Copy, Default, PartialEq)]
pub struct AdmissionSignals {
    /// Event-loop lag EMA in ms (None before the first sample).
    pub lag_ms: Option<u64>,
    /// Process RSS over usable memory, per-mille (None when unknown).
    pub mem_permille: Option<u32>,
    /// Host CPU usage, per-mille 0..1000 (None before the first sample;
    /// display signal, not admission-scored).
    pub cpu_permille: Option<u32>,
}

impl AdmissionSignals {
    /// Sampled memory denominator: cgroup v2 limit when it is smaller
    /// than physical RAM (containers), else physical RAM.
    fn usable_memory_bytes(sys: &sysinfo::System) -> Option<u64> {
        let physical = sys.total_memory();
        let cgroup = Self::read_cgroup_v2_limit();
        Some(
            cgroup
                .filter(|l| *l > 0 && *l < physical)
                .unwrap_or(physical),
        )
    }

    fn read_cgroup_v2_limit() -> Option<u64> {
        let raw = std::fs::read_to_string("/sys/fs/cgroup/memory.max").ok()?;
        raw.trim().parse::<u64>().ok()
    }
}

/// Hysteresis state carried between evaluations.
#[derive(Debug, Clone, Copy, Default, PartialEq)]
pub struct AdmissionState {
    /// Healthy samples in a row (reset on any breach); 0 = overloaded.
    pub consecutive_healthy_secs: u64,
    pub overloaded: bool,
}

/// One admission decision.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Decision {
    /// Overall pressure 0..1000 (per-mille): the max of the enabled
    /// signal ratios — placement orders workers by this.
    pub load_score_permille: u32,
    pub can_accept: bool,
}

impl Decision {
    /// An accepting decision at the given score (cold start / all
    /// signals disabled).
    pub fn accepting(score: u32) -> Self {
        Decision {
            load_score_permille: score.min(1000),
            can_accept: true,
        }
    }
}

/// Pure evaluation — the unit-testable core.
///
/// `healthy_secs_delta` is how much consecutive-healthy time elapsed
/// since the previous evaluation (callers tick it once per second; tests
/// pass exact values).
pub fn evaluate(
    signals: &AdmissionSignals,
    config: &AdmissionConfig,
    state: &mut AdmissionState,
    healthy_secs_delta: u64,
) -> Decision {
    let lag_ratio = ratio(
        signals.lag_ms.map(|l| l as f64).unwrap_or(0.0),
        config.lag_threshold_ms as f64,
    );
    let mem_ratio = signals
        .mem_permille
        .map(|m| m as f64 / 1000.0)
        .unwrap_or(0.0);
    let score = (lag_ratio.max(mem_ratio) * 1000.0).round() as u32;

    let lag_ok = config.lag_threshold_ms == 0
        || signals.lag_ms.is_none()
        || signals.lag_ms.unwrap_or(0) < config.lag_threshold_ms;
    let mem_ok = config.memory_watermark_percent == 0
        || signals.mem_permille.is_none()
        || (signals.mem_permille.unwrap_or(0)) < (config.memory_watermark_percent * 10) as u32;

    if lag_ok && mem_ok {
        state.consecutive_healthy_secs = state
            .consecutive_healthy_secs
            .saturating_add(healthy_secs_delta);
        if state.overloaded && state.consecutive_healthy_secs >= config.cooldown_secs {
            state.overloaded = false;
        }
    } else {
        state.consecutive_healthy_secs = 0;
        state.overloaded = true;
    }

    Decision {
        load_score_permille: score.min(1000),
        can_accept: !state.overloaded,
    }
}

fn ratio(value: f64, threshold: f64) -> f64 {
    if threshold <= 0.0 {
        return 0.0;
    }
    (value / threshold).clamp(0.0, 1.0)
}

/// Shared, lock-free signal store the samplers write and the heartbeat
/// reads. All three fields are optional, so each has a presence bit and a
/// dedicated bit range in the packed word (in-process only: both sides of
/// the wire ship in the same binary):
///   bit 62 = lag present    | bits 0..31  = lag ms (clamped to u32)
///   bit 61 = mem present    | bits 32..46 = mem per-mille
///   bit 60 = cpu present    | bits 47..57 = cpu per-mille
/// The all-zero word means "never measured".
#[derive(Clone, Default)]
pub struct SharedSignals(Arc<AtomicU64>);

const NO_SAMPLE: u64 = 0;
const LAG_PRESENT: u64 = 1 << 62;
const MEM_PRESENT: u64 = 1 << 61;
const CPU_PRESENT: u64 = 1 << 60;

impl SharedSignals {
    pub fn new() -> Self {
        SharedSignals(Arc::new(AtomicU64::new(NO_SAMPLE)))
    }

    fn pack(signals: &AdmissionSignals) -> u64 {
        let mut raw = NO_SAMPLE;
        if let Some(l) = signals.lag_ms {
            raw |= LAG_PRESENT | l.min(u32::MAX as u64);
        }
        if let Some(m) = signals.mem_permille {
            raw |= MEM_PRESENT | ((m.min(1000) as u64) << 32);
        }
        if let Some(c) = signals.cpu_permille {
            raw |= CPU_PRESENT | ((c.min(1000) as u64) << 47);
        }
        raw
    }

    fn unpack(raw: u64) -> AdmissionSignals {
        if raw == NO_SAMPLE {
            return AdmissionSignals::default();
        }
        AdmissionSignals {
            lag_ms: (raw & LAG_PRESENT != 0).then_some(raw & 0xffff_ffff),
            mem_permille: (raw & MEM_PRESENT != 0).then_some(((raw >> 32) & 0x7fff) as u32),
            cpu_permille: (raw & CPU_PRESENT != 0).then_some(((raw >> 47) & 0x7ff) as u32),
        }
    }

    /// Publish the latest signals (samplers call this).
    pub fn store(&self, signals: &AdmissionSignals) {
        self.0.store(Self::pack(signals), Ordering::Relaxed);
    }

    /// Read the latest signals.
    pub fn load(&self) -> AdmissionSignals {
        Self::unpack(self.0.load(Ordering::Relaxed))
    }
}

/// Spawn the samplers: a 1s event-loop-lag watchdog and a memory poll.
/// Both write into `signals`; evaluation happens in
/// [`AdmissionController::decision`] on each heartbeat.
pub fn spawn_samplers(signals: SharedSignals, config: AdmissionConfig) {
    // Event-loop lag watchdog.
    let signals_for_lag = signals.clone();
    tokio::spawn(async move {
        let mut ema: Option<f64> = None;
        const ALPHA: f64 = 0.3;
        loop {
            let before = tokio::time::Instant::now();
            tokio::time::sleep(Duration::from_secs(1)).await;
            let overshoot_ms = (before.elapsed().as_millis() as f64 - 1000.0).max(0.0);
            ema = Some(match ema {
                None => overshoot_ms,
                Some(prev) => ALPHA * overshoot_ms + (1.0 - ALPHA) * prev,
            });
            // Publish lag together with the freshest memory reading.
            let mut fresh = signals_for_lag.load();
            fresh.lag_ms = Some(ema.unwrap_or(0.0) as u64);
            signals_for_lag.store(&fresh);
        }
    });

    // Memory sampler (only when the watermark is enabled). Clones the
    // signal handle for the CPU sampler below BEFORE this async move
    // block captures the original.
    let signals_for_cpu = signals.clone();
    if config.memory_watermark_percent > 0 {
        tokio::spawn(async move {
            let mut sys = sysinfo::System::new();
            loop {
                sys.refresh_memory();
                if let Ok(pid) = sysinfo::get_current_pid() {
                    sys.refresh_processes_specifics(
                        sysinfo::ProcessesToUpdate::Some(&[pid]),
                        true,
                        sysinfo::ProcessRefreshKind::nothing().with_memory(),
                    );
                    if let Some(total) = AdmissionSignals::usable_memory_bytes(&sys) {
                        if total > 0 {
                            if let Some(rss) = sys.process(pid).map(|p| p.memory()) {
                                let mut fresh = signals.load();
                                fresh.mem_permille =
                                    Some(((rss as f64 / total as f64) * 1000.0) as u32);
                                signals.store(&fresh);
                            }
                        }
                    }
                }
                tokio::time::sleep(Duration::from_secs(1)).await;
            }
        });
    }

    // Host CPU sampler (display signal for the console; never scored).
    // sysinfo needs two refreshes with a gap to produce a usage reading.
    tokio::spawn(async move {
        let mut sys = sysinfo::System::new();
        sys.refresh_cpu_usage();
        loop {
            tokio::time::sleep(Duration::from_secs(1)).await;
            sys.refresh_cpu_usage();
            let usage = sys.global_cpu_usage().clamp(0.0, 100.0);
            let mut fresh = signals_for_cpu.load();
            fresh.cpu_permille = Some((usage * 10.0).round() as u32);
            signals_for_cpu.store(&fresh);
        }
    });
}

/// Per-worker admission controller: signals + hysteresis state.
pub struct AdmissionController {
    signals: SharedSignals,
    state: std::sync::Mutex<AdmissionState>,
    config: AdmissionConfig,
}

impl AdmissionController {
    pub fn new(config: AdmissionConfig) -> Self {
        let signals = SharedSignals::new();
        spawn_samplers(signals.clone(), config);
        AdmissionController {
            signals,
            state: std::sync::Mutex::new(AdmissionState::default()),
            config,
        }
    }

    /// Test hook: a controller whose signals are set explicitly and whose
    /// samplers never run.
    pub fn new_manual(config: AdmissionConfig) -> Self {
        AdmissionController {
            signals: SharedSignals::new(),
            state: std::sync::Mutex::new(AdmissionState::default()),
            config,
        }
    }

    /// Test hook: inject signals.
    pub fn set_signals(&self, signals: AdmissionSignals) {
        self.signals.store(&signals);
    }

    /// Current decision; ticks the hysteresis by the wall-clock gap since
    /// the previous call (approximately one heartbeat).
    pub fn decision(&self) -> Decision {
        let signals = self.signals.load();
        let mut state = self.state.lock().unwrap();
        evaluate(&signals, &self.config, &mut state, 1)
    }

    /// Raw signals (for reporting alongside the decision).
    pub fn signals(&self) -> AdmissionSignals {
        self.signals.load()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cfg() -> AdmissionConfig {
        AdmissionConfig::default()
    }

    #[test]
    fn healthy_worker_accepts() {
        let mut state = AdmissionState::default();
        let d = evaluate(
            &AdmissionSignals {
                lag_ms: Some(50),
                mem_permille: Some(300),
                cpu_permille: None,            },
            &cfg(),
            &mut state,
            1,
        );
        assert!(d.can_accept);
        assert!(d.load_score_permille > 0);
    }

    #[test]
    fn lag_breach_overloads_immediately() {
        let mut state = AdmissionState::default();
        let d = evaluate(
            &AdmissionSignals {
                lag_ms: Some(900),
                mem_permille: Some(300),
                cpu_permille: None,            },
            &cfg(),
            &mut state,
            1,
        );
        assert!(!d.can_accept);
    }

    #[test]
    fn memory_breach_overloads_immediately() {
        let mut state = AdmissionState::default();
        let d = evaluate(
            &AdmissionSignals {
                lag_ms: Some(10),
                mem_permille: Some(800),
                cpu_permille: None,            },
            &cfg(),
            &mut state,
            1,
        );
        assert!(!d.can_accept);
    }

    #[test]
    fn recovery_requires_cooldown_window() {
        let mut state = AdmissionState::default();
        // Overload first.
        evaluate(
            &AdmissionSignals {
                lag_ms: Some(900),
                mem_permille: Some(300),
                cpu_permille: None,            },
            &cfg(),
            &mut state,
            1,
        );
        // Healthy again but not for long enough: still refusing.
        let d = evaluate(
            &AdmissionSignals {
                lag_ms: Some(50),
                mem_permille: Some(300),
                cpu_permille: None,            },
            &cfg(),
            &mut state,
            5,
        );
        assert!(!d.can_accept, "cooldown not elapsed yet");
        // After the full cooldown: accepting again.
        let d = evaluate(
            &AdmissionSignals {
                lag_ms: Some(50),
                mem_permille: Some(300),
                cpu_permille: None,            },
            &cfg(),
            &mut state,
            5,
        );
        assert!(d.can_accept);
    }

    #[test]
    fn disabled_signals_never_overload() {
        let mut state = AdmissionState::default();
        let config = AdmissionConfig {
            lag_threshold_ms: 0,
            memory_watermark_percent: 0,
            cooldown_secs: 10,
        };
        let d = evaluate(
            &AdmissionSignals {
                lag_ms: Some(60_000),
                mem_permille: Some(999),
                cpu_permille: None,            },
            &config,
            &mut state,
            1,
        );
        assert!(d.can_accept, "all signals disabled = always accept");
    }

    #[test]
    fn missing_samples_treated_as_healthy() {
        // Cold start: lag not measured yet, memory unknown — accept.
        let mut state = AdmissionState::default();
        let d = evaluate(&AdmissionSignals::default(), &cfg(), &mut state, 1);
        assert!(d.can_accept);
    }

    #[test]
    fn load_score_is_max_of_ratios() {
        let mut state = AdmissionState::default();
        // lag 250/500 = 500‰; mem 300/750 = 400‰ → score 500.
        let d = evaluate(
            &AdmissionSignals {
                lag_ms: Some(250),
                mem_permille: Some(300),
                cpu_permille: None,            },
            &cfg(),
            &mut state,
            1,
        );
        assert_eq!(d.load_score_permille, 500);
    }

    #[test]
    fn shared_signals_roundtrip() {
        let s = SharedSignals::new();
        assert_eq!(s.load(), AdmissionSignals::default());
        s.store(&AdmissionSignals {
            lag_ms: Some(123),
            mem_permille: Some(456),
            cpu_permille: Some(789),
        });
        assert_eq!(
            s.load(),
            AdmissionSignals {
                lag_ms: Some(123),
                mem_permille: Some(456),
                cpu_permille: Some(789),
            }
        );
        s.store(&AdmissionSignals {
            lag_ms: Some(7),
            mem_permille: None,
            cpu_permille: Some(10),
        });
        assert_eq!(s.load().lag_ms, Some(7));
        assert_eq!(s.load().mem_permille, None);
        assert_eq!(s.load().cpu_permille, Some(10));
    }
}
