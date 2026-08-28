# Cluster HA Design — and How It Compares to the Java Zeta Engine

This document explains the cluster high-availability design of this Rust
engine, grounded in a source-level reading of the Java Apache SeaTunnel
(`dev` branch) it re-implements, and states honestly where this engine is
**strictly better**, where it is **at parity by borrowing proven Java
designs**, and where it **deliberately simplifies**.

Status markers: ✅ implemented · 🚧 in progress (planned stage) · 📋 planned.

## Background: what the Java engine actually does (source-verified)

The Java Zeta engine delegates cluster management to Hazelcast:

- **Mastership** = Hazelcast's oldest-member rule. Every master-eligible
  node polls `nodeEngine.getMasterAddress()` every 100 ms
  (`CoordinatorService.checkNewActiveMaster`,
  `seatunnel-engine-server/.../CoordinatorService.java`) and flips its own
  active flag. There is **no quorum**: during a network partition each
  sub-cluster's oldest member is its own master.
- **No split-brain protection.** Exhaustive search of the engine modules
  finds zero `SplitBrainProtection`, zero quorum checks, and no fencing
  tokens in the RPC operations. The only epoch is a node-local scheduler
  counter (`pendingJobScheduleEpoch`) — and its Javadoc records that it
  was added after a stale scheduler thread double-started a job.
- **The real anti-duplicate mechanisms are on the worker**:
  `TaskExecutionService.deployTask` skips redeploying a `TaskGroupLocation`
  that is already executing ("already exists and is active, skipping
  redeploy for master failover recovery"), and the new master probes task
  liveness with `CheckTaskGroupIsExecutingOperation` before restarting
  anything (re-attach instead of restart).
- **Failure detection is deliberately slow.** Shipped configs set
  `hazelcast.max.no.heartbeat.seconds: 180` with a phi-accrual detector.
  The official post-mortem ["Troubleshooting SeaTunnel Cluster
  Split-Brain"](https://dev.to/seatunnel/troubleshooting-seatunnel-cluster-split-brain-a-deep-dive-into-hazelcast-configuration-and-15gc)
  documents a cluster that split-brained 3 times in one month because
  27-second full-GC stalls crossed a 60-second heartbeat timeout — the
  fix was a slower detector and GC tuning, not consensus.
- **A known takeover defect**: on a pure master switch the new
  coordinator does not re-read checkpoint data from storage
  (`CheckpointManager` loads it only when `restoreSourceJobId != null`);
  correctness relies on workers' tasks surviving. If a pipeline must
  restart before the first new checkpoint completes, it restarts from
  empty state.

The conclusion we draw: the Java engine's master/worker *split* is sound
(and is what the Java project itself recommends over its hybrid mode),
but its consistency story is mitigation, not mechanism. This engine keeps
the split and replaces the mitigation with mechanism.

## Where this engine is strictly better

| Dimension | Java Zeta (source facts) | This engine | Status |
|---|---|---|---|
| Leader election | Oldest-member, no quorum, 100 ms polling | openraft majority-quorum election (~1 s); dual masters are impossible by construction, not by timeout | 📋 Stage 3 |
| Split-brain protection | None anywhere in the engine | Raft quorum for all durable state; minority side cannot commit | 📋 Stage 3 |
| Stale-master fencing | None (only a node-local scheduler epoch) | Monotonic `term` carried on every master↔worker message; workers reject dispatch/cancel/preempt from a lower term (a deposed master cannot disturb tasks) | ✅ Stage 1 (wire protocol + worker-side rejection; term source becomes the Raft leader term in Stage 3) |
| Failover speed vs. false positives | 180 s tolerance chosen to avoid false splits (slow by design) | Quorum makes fast failover safe (~1 s election); worker liveness uses separate soft/hard timeouts (soft: no new assignments; hard: eviction) | ✅ Stage 1 (timeouts); 📋 Stage 3 (election) |
| State healing after a partition | Hazelcast IMap last-write-wins merge, no engine callback — divergent state can silently win or lose | Single replicated log (Raft); no divergent branches exist to merge | 📋 Stage 3 |
| Checkpoint after a master switch | In-memory `latestCompletedCheckpoint` is null until the next checkpoint completes; a pipeline restarting in that window starts from empty state | Recovery always re-reads the latest persisted checkpoint from storage — no empty-state window | 📋 Stage 2 |
| Checkpoint ID uniqueness across failover | Persisted IMap counter, `setCount(id + 1)` on restore (good design) | Same semantics, persisted in the consensus-replicated coordinator state | 📋 Stage 2 |
| Single-machine deployment | Hazelcast cluster semantics and role config even for one node | Three tiers: `seatunnel run -m local` (zero server), single-voter Raft (local commit, no network), `--role hybrid` (one process = full cluster) | ✅ Stage 1 (`--role hybrid`); 📋 Stage 3 (single-voter Raft) |
| Operational identity | `ClusterInfo` leader is effectively hardcoded | Real advertise address, role, and term in `ClusterInfo`; masters never guess `127.0.0.1` for HA sync | ✅ Stage 1 |

## Where this engine is at parity (borrowed Java designs)

These are mechanisms the Java engine got right; we implement the same
semantics, adapted to the pull-based protocol:

- **Adopt (re-attach) before preempt.** On master switch or worker
  reconnect, tasks still assigned to the returning worker are re-marked
  `Running` for it; only tasks reassigned to another worker are fenced.
  This mirrors Java's `deployTask` dedupe + `CheckTaskGroupIsExecuting`
  probe. ✅ Stage 1.
- **Checkpoint IDs never rewind.** Java's `StateStoreCheckpointIDCounter`
  → per-pipeline monotonic counter in coordinator state (replicated, so
  failover cannot reissue an id). 📋 Stage 2.
- **Sink two-phase commit.** Java's `prepareCommit` → coordinator
  persists `CompletedCheckpoint` → `notifyCheckpointComplete` commits →
  our coordinated per-pipeline checkpoint protocol reuses the existing
  `SinkCommitter`/`execute_barrier` machinery for the same semantics.
  📋 Stage 2.
- **Conservative worker eviction.** Soft (30 s, no new assignments) and
  hard (60 s, evict + reclaim) thresholds, both configurable — the same
  lesson the Java post-mortem taught, encoded as two levels instead of
  one giant timeout. ✅ Stage 1.
- **Graceful shutdown releases tasks.** `UnregisterWorker` now evicts the
  worker's tasks so failover starts immediately instead of waiting out
  the hard timeout. ✅ Stage 1.

## Where this engine deliberately simplifies (not claimed as better)

- **No data-plane barrier injection.** Every task is a fully chained
  Source→Transforms→Sink pipeline; there are no inter-task data edges, so
  Chandy-Lamport barrier propagation would be complexity with zero
  benefit. "Alignment" here means coordinated per-pipeline checkpoints
  (all parallel subtasks of a pipeline cut together, master-orchestrated
  two-phase commit) — not barriers travelling through data streams.
  (Stage 2.)
- **Static slot budget** (`slot-num`, default 8) instead of Java's
  memory-based dynamic slot allocation. Scheduling fairness comes from
  least-loaded placement (Stage 4), not resource accounting.
- **Pull-based dispatch over heartbeat (now with a long-poll fast path)**
  instead of Hazelcast operation push. Cost: milliseconds of dispatch
  latency in exchange for workers keeping outbound-only connections
  (NAT/firewall-friendly). (Stage 4.)
- **Static voter membership** (the ordered member list doubles as the
  Raft voter set). Online membership change via joint consensus is out
  of scope; nodes are added by config change + rolling restart.

## Deployment topologies

```
# Single machine, no server at all (unchanged):
seatunnel run -c job.yaml -m local

# Single machine, full cluster in one process (Stage 1):
seatunnel-engine-server --role hybrid --addr 0.0.0.0:5800

# Symmetric multi-node (recommended HA, Qdrant-style all-peers):
#   3 identical hybrid nodes; Raft elects the coordinator
seatunnel-engine-server --role hybrid --addr 0.0.0.0:5800 \
  --advertise-addr node1:5800 -f config/seatunnel-3node.yaml   # ×3

# Separated (large clusters, Java parity):
#   3 masters (Raft voters) + N workers
seatunnel-engine-server --role master  --addr 0.0.0.0:5800 ...
seatunnel-engine-server --role worker  --master m1:5800,m2:5800,m3:5800 ...
```

Scaling rule: master/hybrid (voter) counts go 1 → 3 → 5. **Two voters
are rejected** — a two-node majority cannot survive either node's
failure, and silently accepting it would reintroduce the dual-master risk
this design exists to eliminate (the old warm-standby setup could run
two writable masters; it was documented as a known limitation).

## Roadmap

- **Stage 1 (done)** — fencing terms on the wire + worker-side
  rejection; soft/hard liveness; `--role hybrid`; real cluster identity;
  adopt-before-preempt; dead-code removal (the superseded
  `leader_election`/`job_manager`/`resource_manager` stubs and unused
  checkpoint backends).
- **Stage 2** — coordinated per-pipeline checkpoint two-phase commit with
  master-assigned monotonic IDs; restore always re-reads storage.
- **Stage 3** — openraft takes over HA: coordinator state becomes a Raft
  state machine (snapshot = the existing `export_state` JSON); the Raft
  leader term becomes the wire fencing term; snapshot-polling
  replication and `ReplicationService` are removed.
- **Stage 4** — long-poll dispatch; least-loaded slot placement;
  term/leader/Raft observability; benchmarks and this document's final
  revision with measured numbers.
