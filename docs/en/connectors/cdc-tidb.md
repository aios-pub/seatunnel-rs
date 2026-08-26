# TiDB CDC Connector

Change data capture **directly from TiKV** via the CDC EventFeedV2 gRPC
protocol (PD discovery, memcomparable key ranges, rowcodec v1/v2 decoding,
Percolator transaction assembly) — no TiCDC server needed. Snapshot reads
go through the MySQL-compatible SQL endpoint. Official 2.3.x options are
supported.

## Configuration

```yaml
source:
  TiDB-CDC:
    url: "jdbc:mysql://127.0.0.1:4000/seatunnel"   # official SQL-endpoint form
    username: root
    password: ""
    pd-addresses: "127.0.0.1:2379"                 # official key (pd-addrs alias)
    database-name: seatunnel
    table-name: users
    startup.mode: initial
    batch-size-per-scan: 1000
    timeout: 30000
```

### Options

| Option | Default | Notes |
| --- | --- | --- |
| `url` | — | `jdbc:mysql://host:port/db` for the SQL endpoint; conn-host/conn-port remain as alternatives |
| `username` / `password` | — | SQL endpoint credentials |
| `pd-addresses` | 127.0.0.1:2379 | PD endpoints (comma list; `pd-addrs` alias) |
| `database-name` / `table-name` | — | single captured table |
| `startup.mode` | initial | `initial` \| `earliest` \| `latest` \| `timestamp` \| `specific` |
| `startup.timestamp` | — | ms since epoch; converted to a TSO (`ms << 18`) MVCC scan point (must be inside the GC lifetime) |
| `startup.specific-offset.pos` | — | a TSO used directly as `checkpoint_ts` for `startup.mode = specific` |
| `batch-size-per-scan` | 1000 | snapshot page size per scan |
| `timeout` / `tikv.grpc.timeout_in_ms` | engine default | bounds each change-stream poll cycle |
| `tikv.grpc.scan_timeout_in_ms` | engine default | bounds each snapshot-side poll cycle |
| `tikv.batch_get_concurrency` / `tikv.batch_scan_concurrency` | — | accepted for compatibility; concurrency follows the per-region stream model |
| `store-address-rewrite` | — | `from=to,...` rewrites store addresses TiKV advertises (Docker setups) |
| `resubscribe-interval-ms` | 0 (off) | periodic re-registration of region streams |
| `split.column` | id | snapshot chunking key |

### Accepted for compatibility

`exactly_once`, `format`, `debeziumConfig` and the chunk-key/sampling
options are accepted and logged, following the Rust implementation's
behavior.

## How it works

1. Resolves the table id from the SQL endpoint, derives the TiKV record
   key range (`t{id}_r` … `t{id}_s`, memcomparable-encoded for PD).
2. Snapshot: parallel keyset ranges per subtask over the SQL endpoint.
3. Incremental (subtask 0): one EventFeedV2 stream per region from TiKV;
   percolator prewrite/commit matching, resolved-ts driven watermarking,
   automatic region split/merge re-subscription.
4. Checkpoint: resolved_ts (TSO) + snapshot cursors.

TiKV streams no DDL, so schema evolution polls `information_schema` on an
interval and refreshes the decode schema in place — see
[schema evolution](../schema-evolution.md).

Delivery semantics: at-least-once.
