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
| Leader election | Oldest-member, no quorum, 100 ms polling | openraft majority-quorum election (~1 s); dual masters are impossible by construction, not by timeout | ✅ Stage 3 |
| Split-brain protection | None anywhere in the engine | Raft quorum for all durable state; minority side cannot commit | ✅ Stage 3 |
| Stale-master fencing | None (only a node-local scheduler epoch) | Monotonic `term` carried on every master↔worker message; workers reject dispatch/cancel/preempt from a lower term (a deposed master cannot disturb tasks) | ✅ Stage 1 (wire protocol + worker-side rejection; term source becomes the Raft leader term in Stage 3) |
| Failover speed vs. false positives | 180 s tolerance chosen to avoid false splits (slow by design) | Quorum makes fast failover safe (~1 s election); worker liveness uses separate soft/hard timeouts (soft: no new assignments; hard: eviction) | ✅ Stage 3 |
| State healing after a partition | Hazelcast IMap last-write-wins merge, no engine callback — divergent state can silently win or lose | Single replicated log (Raft); no divergent branches exist to merge | ✅ Stage 3 |
| Checkpoint after a master switch | In-memory `latestCompletedCheckpoint` is null until the next checkpoint completes; a pipeline restarting in that window starts from empty state | Checkpoint state lives in the durable stores at prepare time; the master only decides ids and resolution — no empty-state window after any switch | ✅ Stage 2 |
| Checkpoint ID uniqueness across failover | Persisted IMap counter, `setCount(id + 1)` on restore (good design) | Same semantics: per-job counter exported with the HA snapshot, `max()` on import so ids never rewind | ✅ Stage 2 |
| Cluster-mode checkpoint semantics | Per-pipeline CheckpointCoordinator with barrier injection through real DAG edges | Master-driven coordinated checkpoints reusing the exact local-mode 2PC (prepare → master persists/collects → complete → sink commit); no data-plane barriers because tasks are fully chained | ✅ Stage 2 |
| Single-machine deployment | Hazelcast cluster semantics and role config even for one node | Three tiers: `seatunnel run -m local` (zero server), single-voter Raft (local commit, no network), `--role hybrid` (one process = full cluster) | ✅ Stage 3 |
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
  → per-job monotonic counter in coordinator state (replicated, so
  failover cannot reissue an id). ✅ Stage 2.
- **Sink two-phase commit.** Java's `prepareCommit` → coordinator
  persists `CompletedCheckpoint` → `notifyCheckpointComplete` commits →
  our coordinated per-pipeline checkpoint protocol reuses the existing
  `SinkCommitter`/`execute_barrier` machinery for the same semantics.
  ✅ Stage 2.
- **"Alignment" without barrier injection.** All parallel subtasks of a
  pipeline cut at one master-assigned checkpoint id; resolution (phase 2)
  happens only after every participant prepared — the meaningful form of
  alignment for fully-chained tasks. ✅ Stage 2.
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
- **No slot mechanism at all.** Java allocates memory-sliced slots and
  blocks JobMasters waiting for them; here "is the worker full" is
  answered by MEASURED pressure — event-loop lag and a memory watermark
  (with hysteresis) — reported on every heartbeat. Overloaded workers
  simply receive nothing new and their pending tasks are stolen by
  healthy peers; when everyone is over a watermark, tasks queue as
  SCHEDULED (no blocking waits). Placement orders workers by the measured
  load score. The verdict and both signals are visible in the web
  console's cluster page and as Prometheus gauges — the mechanism is
  meant to be seen, not guessed.
- **Pull-based dispatch over heartbeat with a long-poll fast path**
  (✅ Stage 4) instead of Hazelcast operation push. Workers ask with a
  `wait_ms` budget; the master parks the request on a wake signal and
  answers the instant dispatchable work, cancellations, fences or
  checkpoint resolutions appear — dispatch latency drops from a full
  heartbeat interval (≤2 s) to ~0 while connections stay
  worker-initiated (NAT/firewall-friendly).
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

Helper scripts: `scripts/start-hybrid.sh` (single-node hybrid) and
`scripts/start-hybrid-cluster.sh` (the 3-node symmetric topology on
localhost) build, start and wait for leader readiness;
`config/seatunnel-3node.yaml` is the reference config for a real
3-machine deployment.

Known limitation (task failover races): when a node dies, its tasks are
re-claimed through a read-then-`MarkDispatched` sequence that has no
cross-worker mutual exclusion — concurrent heartbeat claims can both
dispatch a task (one copy is fenced), and a fenced copy's late reports
can briefly clobber the survivor's state. Re-election, worker
re-registration and job submission remain solid; a job that stalls
after a failover event should be resubmitted (same job id restores from
the latest checkpoint). A proper fix needs claim arbitration that
commits liveness with ownership (single-writer `ClaimTask` command
instead of the local-registry classify + replicated mark).

Scaling rule: master/hybrid (voter) counts go 1 → 3 → 5. **Two voters
are rejected at startup** — a two-node majority cannot survive either
node's failure, and silently accepting it would reintroduce the
dual-master risk this design exists to eliminate (the pre-Raft
warm-standby setup could run two writable masters; it was documented as
a known limitation). A degraded two-node pair is not a fallback either:
it tolerates zero failures, which is strictly worse than the single-vote
hybrid while giving up Raft's fencing. If only two machines exist, run a
lightweight third voter (a hybrid node without jobs costs almost
nothing — the control plane is a few KB of JSON log). When two of three
voters are down, the lone survivor correctly stops electing (no quorum);
it accumulates harmless term bumps while campaigning into the void, and
the cluster recovers as soon as a second voter returns. Voter ids come
from the ordered member list (position 1..n); node 1 bootstraps the
membership, and if it never appears, another node retries initialization
after 5 s.

## Election stability (application layer)

openraft 0.9.x has no Pre-Vote, CheckQuorum or leader-priority knobs
(pre-vote lands behind `Config::enable_pre_vote` in 0.10, which is still
alpha with a breaking storage/network API). Election thrashing was
therefore fixed at the application layer, in three parts:

1. **Per-node disjoint election windows** (the core fix). openraft draws
   the election timeout once per process from
   `[election_timeout_min, election_timeout_max)` and never re-rolls it,
   and every follower counts from the leader's last heartbeat — so two
   survivors that draw close values campaign in lockstep forever: each
   round bumps the term and nobody wins. This engine gives voter *i*
   (member-list position, 1-based) the window
   `[min+(i-1)·skew, max+(i-1)·skew)`. Defaults: min/max = 900/1300 ms,
   skew = 700 ms (gap between adjacent windows = 300 ms > one openraft
   tick of 225 ms), heartbeat 150 ms — so voter 1 draws from
   [900,1300), voter 2 from [1600,2000), voter 3 from [2300,2700). The
   lowest live voter always fires first and the others grant its vote:
   one round, term +1, done. Member-list order is the leader priority
   order (the same order workers already use for failover). Normal
   followers never reject votes on a lease — only a leader with a
   committed vote does — so the skew cannot deadlock an election. Knobs
   (`seatunnel.engine.raft.*`: `election-timeout-min-ms`,
   `election-timeout-max-ms`, `election-skew-ms` (0 disables),
   `heartbeat-interval-ms`); a positive skew too small to separate the
   windows is lifted to window-width + one tick at load, with a warning.

2. **Cached raft RPC connections.** openraft hard-times-out append RPCs
   at `heartbeat_interval` (150 ms); the transport used to open a fresh
   TCP+HTTP/2 connection per RPC, and a handshake that misses 150 ms is
   reported Unreachable — which ages followers and triggers the very
   elections it mimics. The network layer now keeps one multiplexed
   tonic `Channel` per target (250 ms connect timeout), invalidated on
   any RPC failure so a restarted peer is re-dialed.

3. **fsync off the async runtime.** The log store's append/vote writes
   and the state machine's snapshot writes run inside
   `spawn_blocking`; a blocking reactor used to delay raft ticks and
   heartbeats, compounding both problems above.

Client availability during the failover window: mutating RPCs carry the
leadership gate `"not the leader; retry at <addr>"`, and
`seatunnel-engine-client` follows that hint (30 s budget, re-walking the
configured master list if a hinted leader is unreachable), so a submit
issued mid-election succeeds instead of erroring. A follower's
`register_worker` now answers with the real leader address instead of
itself.

Measured on the 3-node pseudo-cluster (`scripts/start-hybrid-cluster.sh`,
debug build), streaming job running, `kill -9` on the leader: both
failovers completed with exactly one term increment (1→2, 44→45) and no
further campaigns — re-election ≈ 2–4 s (window + debug-build tick lag;
release builds are faster), workers re-registered on the new leader
within seconds and the job kept running. Term inflation only appears in
the deliberately quorum-less window (two of three voters dead), where it
is harmless: nobody grants, nothing commits.

## Roadmap

- **Stage 1 (done)** — fencing terms on the wire + worker-side
  rejection; soft/hard liveness; `--role hybrid`; real cluster identity;
  adopt-before-preempt; dead-code removal (the superseded
  `leader_election`/`job_manager`/`resource_manager` stubs and unused
  checkpoint backends).
- **Stage 2 (done)** — coordinated per-pipeline checkpoint two-phase
  commit with master-assigned monotonic IDs; triggers and resolutions
  ride heartbeats; abort on failure/timeout (`checkpoint-timeout-ms`);
  exit barriers now persist resumable state in cluster mode too.
- **Stage 3 (done)** — openraft takes over HA: the coordinator's durable
  mutations are a `Command` log applied deterministically on every node
  (snapshot = the existing `export_state` JSON); the Raft leader term is
  the wire fencing term; `ReplicationService` and the ordered-list
  warm-standby polling are removed; voter counts are validated (1/3/5,
  two rejected loudly). Fast, collision-free election via per-node
  disjoint windows (0.9–2.7 s across three voters) replaces the Java
  engine's 180 s tolerance — safe because a quorum, not a timeout,
  decides leadership.
- **Stage 4 (done)** — long-poll dispatch (`wait_ms` + wake signal on
  every instruction-producing event); least-loaded slot placement
  (saturated workers skipped, permissive fallback when all are full);
  `cluster` CLI shows term/leader/role/slots; this document finalized.
  Quantified latency benchmarks (Stage-4 acceptance item) are left as
  follow-up work alongside the existing `seatunnel-benchmarks` suite —
  the long-poll path is covered by the cluster integration tests, not
  yet by a latency benchmark.
- **Follow-up: openraft 0.10 once stable** — enable protocol-level
  Pre-Vote (`Config::enable_pre_vote`) and the redesigned vote semantics
  (a split vote no longer forces a new term); revisit whether the
  per-node window skew can then be shrunk or dropped. Until then the
  application-layer measures above are the stability mechanism; claim
  arbitration for task failover (single-writer `ClaimTask`) remains the
  other open item.
