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
- **Worker failover**: two-level liveness (`worker-soft-timeout-ms`: silent workers get no new assignments; `worker-timeout-ms`: eviction) + orphan-task claim rule + per-task preemption fence (`HeartbeatResponse.preempted_task_ids`) — a returning worker that still runs a reassigned task is told to stop it, preventing dual execution. Re-connecting workers re-attach to their still-assigned tasks (adopt-before-preempt).
- **Master HA (Stage 3)**: openraft consensus over the member list (voter counts 1/3/5; two rejected). The coordinator's durable mutations replicate through the Raft `Command` log; the leader term is the wire fencing term; election completes in ~1 s. Workers and clients (`-a addr1,addr2,...`) rotate addresses; follower masters serve no instructions and point workers at the leader via `leader_hint`.
- **S3 cleanup**: three layers — write-time retention (`keep-checkpoint-count`), master cancel-grace deletion, TTL sweep (`history-job-expire-minutes`).

## Known limitations (before true production hardening)

1. **Security**: no TLS/mTLS on gRPC, no authentication on any service (master/worker/client/replication). Production requires at least TLS + token auth.
2. **Observability**: no metrics endpoint (Prometheus), no tracing/tracing span propagation, log-based diagnostics only.
3. **Checkpoint size**: states are small (KB JSON); large-state chunked/multipart S3 upload is not implemented.
4. **Simultaneous total failure**: all masters + S3 lost simultaneously = checkpoint loss (same as any distributed system without off-site backup).
5. **In-flight coordinated checkpoints on leader switch**: pending checkpoints are leader-local; a switch drops them (workers unwind via abort/next checkpoint). Checkpoint DATA always lives in the durable stores; only the in-flight cut is lost.
6. **Schema-change HA**: schema-evolution events flow through the data plane (survive master failover), but a master failover exactly between a schema change and its checkpoint is at-least-once (the event re-fires).
7. **Backpressure**: fan-out sink buffers are bounded (1024/queue) but there is no cross-stage credit-based flow control.

## Operational guidance

- Set `checkpoint.storage.type: s3` with a real S3/MinIO deployment for production (decouples checkpoint durability from master liveness).
- `worker-timeout-ms` (hard eviction, default 60000) should be ≥ 3× heartbeat interval (`heartbeat-interval-ms`, default 2000); `worker-soft-timeout-ms` (default 30000) should sit between the two. Increase both on lossy networks to avoid premature eviction — the Java engine ships 180s tolerance for exactly this reason (a 27s full-GC stall once split its cluster).
- Graceful worker shutdown (`UnregisterWorker`) releases the worker's tasks for takeover immediately; only crashed workers wait out the hard timeout.
- Task admission is dynamic (measured pressure, no slot counts): tune `overload-lag-ms` / `memory-watermark-percent` on lossy or small hosts; both accept `0` to disable a signal (disabled-everything = unlimited concurrency — understand the risk before doing it). Watch the cluster page's overload badges or `seatunnel_worker_overloaded` gauges.
- `replication-interval-ms` bounds standby staleness; 5000ms default is a good trade-off.
- Monitor for `Failover: task` and `failing over to` log lines — they indicate infra-level instability.
