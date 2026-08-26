# Kafka Connector

## Source

```toml
source {
  kafka {
    bootstrap.servers = "localhost:9092"
    topic = "my-topic"
    format = "json"
    startup.mode = "earliest"
    # also: latest | group-offsets | timestamp (with startup.timestamp)
    #       | specific-offsets (with startup.specific-offsets: "0:100,1:250")
  }
}
```

## Sink

```toml
sink {
  kafka {
    bootstrap.servers = "localhost:9092"
    topic = "my-sink"
    format = "json"
    transactions.enabled = true
  }
}
```
