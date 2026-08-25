# Kafka Connector

## Source

```toml
source {
  kafka {
    bootstrap.servers = "localhost:9092"
    topic = "my-topic"
    format = "json"
    startup.mode = "earliest"
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
