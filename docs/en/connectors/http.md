# HTTP Connector

REST source and sink over `reqwest` (Java: `connector-http`).

## Source

Fetches an endpoint once (`poll-interval-ms: 0`, bounded) or polls it on
an interval (unbounded). JSON responses map to positional rows: an array
yields one row per element, an object a single row. `data-path` locates
the row array inside a wrapper document. Server errors (5xx/429) and
network failures are retried up to `max-retries`.

```yaml
source:
  Http:
    url: https://api.example.com/v1/items
    method: GET                       # GET (default) / POST / PUT / PATCH ...
    # body: '{"page": 1}'             # request body for POST/PUT/PATCH
    headers:                          # nested keys flatten to headers.Name
      Authorization: "Bearer token"
    poll-interval-ms: 0               # 0 = single fetch; >0 = poll loop
    format: json                      # json | text
    data-path: data.items             # dotted path to the row array
    timeout-ms: 30000
    max-retries: 3
    # columns: "id,name"              # map JSON objects by these names
```

Without `columns`, JSON objects map sorted keys to positions. Polling
sources keep no server-side cursor: a restart re-fetches the current
document, which can duplicate rows (documented at-least-once).

## Sink

POSTs rows as JSON. `max-batch-size: 1` (default) sends one request per
row (the row as a JSON object); larger batch sizes group rows into a
JSON array or NDJSON body per request. Success is a 2xx response.

```yaml
sink:
  Http:
    url: https://api.example.com/v1/collect
    method: POST
    headers:
      Authorization: "Bearer token"
    max-batch-size: 1                 # >1 enables batched bodies
    batch-format: json-array          # json-array (default) | ndjson
    batch.timeout.ms: 100
    timeout-ms: 30000
    max-retries: 3
```

Field names come from the propagated schema or positional `fN`
placeholders. Raw binary `Bytes` fields are hex-encoded in JSON.
