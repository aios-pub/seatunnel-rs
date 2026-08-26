# TiDB CDC Connector

Change data capture **directly from TiKV** via the CDC EventFeedV2 gRPC
protocol (PD discovery, memcomparable key ranges, rowcodec v1/v2 decoding,
Percolator transaction assembly) — no TiCDC server needed. Snapshot reads
go through the MySQL-compatible SQL endpoint.

## Configuration

```yaml
source:
  TiDB-CDC:
    pd-addrs: "127.0.0.1:2379"      # Placement Driver endpoints
    database-name: seatunnel
    table-name: users
    split.column: id
    # MySQL-compatible SQL endpoint (snapshot + metadata)
    conn-host: "127.0.0.1"
    conn-port: 4000
    conn-user: root
    conn-password: ""
    conn-database: seatunnel
    startup.mode: initial           # initial | earliest | latest | timestamp
    # startup.timestamp: 1667232000000   # ms, for startup.mode=timestamp
    # Rewrites store addresses TiKV advertises (Docker setups):
    # store-address-rewrite: "host.docker.internal=127.0.0.1"
    # Re-registration interval ms (0 = disabled)
    # resubscribe-interval-ms: 0
    # schema-evolution.enabled: true
```

## How it works

1. Resolves the table id from the SQL endpoint, derives the TiKV record
   key range (`t{id}_r` … `t{id}_s`, memcomparable-encoded for PD).
2. Snapshot: parallel keyset ranges per subtask over the SQL endpoint.
3. Incremental (subtask 0): one EventFeedV2 stream per region from TiKV;
   percolator prewrite/commit matching, resolved-ts driven watermarking,
   automatic region split/merge re-subscription.
4. Checkpoint: resolved_ts (TSO) + snapshot cursors.

### Startup modes

`initial` (snapshot + stream), `earliest`/`latest` (stream only), and
**`timestamp`**: the wall-clock start time is converted to a TSO
(`ms << 18`) and used as the MVCC `checkpoint_ts`, so TiKV replays commits
from that point (must be within the GC lifetime). See
[startup modes](../startup-modes.md).

TiKV streams no DDL, so schema evolution polls `information_schema` on an
interval and refreshes the decode schema in place — see
[schema evolution](../schema-evolution.md).

Delivery semantics: at-least-once.
