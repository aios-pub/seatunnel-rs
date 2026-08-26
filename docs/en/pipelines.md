# Multi-Pipeline Jobs (Multi-Source / Multi-Sink / Fan-Out)

One job config can declare **multiple pipelines**, each pairing a source
with its own sink list — including **fan-out**: a single source
broadcasting to several different sink systems concurrently through one
read connection.

## Syntax

```yaml
env:
  job.name: demo
  parallelism: 2                  # default parallelism for every pipeline
  checkpoint.interval: 5000
  on-sink-failure: fail           # default fan-out failure policy

pipelines:
  - name: cdc-fanout              # optional, used in logs / task names
    parallelism: 1                # optional per-pipeline override
    source:
      MySQL-CDC: { ... }
    sinks:                        # fan-out: one reader → N concurrent writers
      - Kafka: { ... }
      - JDBC: { ... }
    # on-sink-failure: isolate    # per-pipeline override

  - name: kafka-sync              # a second, independent source
    source:
      Kafka: { ... }
    sinks:
      - Redis: { ... }

  - name: export                  # a third source reusing a sink config
    source:
      JDBC: { ... }
    sinks:
      - Kafka: { ... }
```

Each pipeline is compiled into its own set of tasks (parallelism ×
round-robin over workers) with **its own checkpoints** — task ids are
`{job}-p{pipeline}-{subtask}`. The example user story *source₁→sink₁,
source₂→sink₂, source₃→sink₁* is simply three pipelines; one source
fanning out to several sinks is one pipeline with several `sinks`.

### Backward compatibility

The classic single-pipeline form keeps working unchanged and now also
accepts a sink list:

```yaml
source:
  MySQL-CDC: { ... }
sink:
  Kafka: { ... }
# or fan-out with the legacy layout:
# sink:
#   - Kafka: { ... }
#   - JDBC: { ... }
```

## Fan-out execution model (low latency)

Inside a pipeline task the sinks are wrapped by a fan-out multiplexer:

```
reader ──► FanoutSinkWriter ──┬── channel A (1024) ──► writer task A
                              ├── channel B (1024) ──► writer task B
                              └── channel C (1024) ──► writer task C
```

- each sink runs on its **own tokio task** with a bounded queue;
- `write` only **enqueues** — one sink's slow I/O never delays the
  others or the reader until its buffer fills (natural backpressure);
- **checkpoints keep their order**: the multiplexer broadcasts a flush,
  awaits every sink's ack, and only then the reader snapshot is taken
  (at-least-once preserved);
- **schema changes** are forwarded to every sink and acked before any
  row with the new shape is written;
- transforms run once per pipeline (before the fan-out), not per sink.

## Failure policy (`on-sink-failure`)

| Value | Behavior |
| --- | --- |
| `fail` (default) | A sink failure fails the whole task — strict, replay-safe. |
| `isolate` | The failed sink is removed (logged as ERROR); the remaining sinks continue. A restart replays the missing rows from the reader checkpoint (at-least-once, best-effort continuity). |

Set the default under `env.on-sink-failure` and override per pipeline
with `on-sink-failure` inside the pipeline entry.

## End-to-end verification

`scripts/e2e-multi-pipeline.sh` runs a live two-pipeline job against the
compose cluster: MySQL-CDC fanning out to Kafka + JDBC while a JDBC
source pipeline exports to the console — asserting snapshot and live
rows reach BOTH fan-out sinks, then cancelling cleans up all tasks.
