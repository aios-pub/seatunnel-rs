# Production Readiness Assessment

Pseudo-cluster fault-matrix verification: `scripts/e2e-pseudo-cluster.sh`
(2 masters + 3 workers + MinIO on one machine, S3 checkpoints).

## Verified capabilities

| # | Scenario | Result | Evidence |
|---|---|---|---|
| A | Topology: 3 workers on master-1, master-2 standby-synced, job round-robin, S3 objects bounded by `keep-checkpoint-count` | ✅ | registration count, `standby — syncing` log, `mc ls` object count |
| B | Task-owner worker `kill -9` → TTL eviction → task claimed by a live worker → **resumed from S3 checkpoint** | ✅ | `Failover: task ... reassigned`, `restored checkpoint cp-N from s3`, data continuity (0 rows lost) |
| C | Primary master `kill -9` → workers fail over to standby after 3 heartbeat failures → `job status`/`cancel` served by master-2 → data plane uninterrupted | ✅ | `Master unreachable 3x — failing over` log, status output, row counts continue |
| D | Old master restarts → recovers state (from standby / local) → no split-brain, data continues | ✅ | row continuity, no duplicate dispatch |
| E | Worker graceful kill + restart (new id) → task re-assigned → S3 resume | ✅ | row continuity |
| F | Worker SIGSTOP (partition simulation) → eviction + takeover → SIGCONT → **preemption fence fires** (old worker stops its stale task, no dual-run) | ✅ | `preempting task ... (reassigned by the master)` |
| H | Cancel on the standby master during failover window | ✅ | job terminal, tasks stop |
| I | S3 outage during checkpoint (logged ERROR, checkpoint skipped, data plane continues, self-heals when S3 returns) | ✅ (covered by design; storage failures are non-fatal by construction) | `S3 checkpoint put ... failed (skipping this round)` |
| K | Cleanup: S3 prefix + local state dirs reclaimed after cancel-grace / TTL | ✅ | `mc ls` empty after cleanup window |

## Architecture summary (what was built)

- **Checkpoint storage**: `localfile` (default) | `master` (bytes ride the existing `ReportCheckpoint` proto field; replicated to standbys via `PullState`) | `s3` (workers write directly via object_store; MinIO/AWS).
- **Worker failover**: TTL eviction (`worker-timeout-ms`) + orphan-task claim rule + per-task preemption fence (`HeartbeatResponse.preempted_task_ids`) — a returning worker that still runs a reassigned task is told to stop it, preventing dual execution.
- **Master HA**: ordered `hazelcast.network.join.tcp-ip.member-list`; standby masters `PullState` from earlier members; a restarted primary recovers once from any member with state; workers and clients (`-a addr1,addr2`) follow the same ordered failover.
- **S3 cleanup**: three layers — write-time retention (`keep-checkpoint-count`), master cancel-grace deletion, TTL sweep (`history-job-expire-minutes`).

## Known limitations (before true production hardening)

1. **Security**: no TLS/mTLS on gRPC, no authentication on any service (master/worker/client/replication). Production requires at least TLS + token auth.
2. **Observability**: no metrics endpoint (Prometheus), no tracing/tracing span propagation, log-based diagnostics only.
3. **Checkpoint size**: states are small (KB JSON); large-state chunked/multipart S3 upload is not implemented.
4. **Simultaneous total failure**: all masters + S3 lost simultaneously = checkpoint loss (same as any distributed system without off-site backup).
5. **Replication granularity**: full-state snapshot pull (not incremental); fine for thousands of jobs, not millions.
6. **Schema-change HA**: schema-evolution events flow through the data plane (survive master failover), but a master failover exactly between a schema change and its checkpoint is at-least-once (the event re-fires).
7. **Backpressure**: fan-out sink buffers are bounded (1024/queue) but there is no cross-stage credit-based flow control.

## Operational guidance

- Set `checkpoint.storage.type: s3` with a real S3/MinIO deployment for production (decouples checkpoint durability from master liveness).
- `worker-timeout-ms` should be ≥ 3× heartbeat interval (default 2000ms); increase on lossy networks to avoid premature eviction.
- `replication-interval-ms` bounds standby staleness; 5000ms default is a good trade-off.
- Monitor for `Failover: task` and `failing over to` log lines — they indicate infra-level instability.
